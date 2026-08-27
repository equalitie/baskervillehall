# -*- coding: utf-8 -*-
"""
Baskervillehall Predictor (single-threaded version)

- Simple single-threaded processing for container environments
- Scale by increasing number of pods in StatefulSet
- No multiprocessing overhead or complexity
"""

import json
import logging
import os
import threading
import time as time_module
import statistics
from collections import defaultdict, deque, Counter
from datetime import datetime
from queue import Queue, Full
from typing import List, Tuple, Dict, Any

import requests as http_requests

import psycopg2

from cachetools import TTLCache
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from baskervillehall.baskervillehall_isolation_forest import (
    BaskervillehallIsolationForest,
    ModelType,
)
from baskervillehall.baskerville_rules import detect_scraper, is_human
from baskervillehall.model_storage import ModelStorage
from baskervillehall.settings_deflect_api import SettingsDeflectAPI
from baskervillehall.settings_postgres import SettingsPostgres
from kafka.errors import NoBrokersAvailable
from kafka.consumer.subscription_state import ConsumerRebalanceListener

CLUSTER_CHECK_INTERVAL = 300    # seconds between checks per host
CLUSTER_MIN_SESSIONS = 10       # minimum sessions to analyze
CLUSTER_MIN_UNIQUE_IPS = 3      # minimum unique IPs — single crawler never triggers an incident
CLUSTER_ALERT_THRESHOLD = 0.55  # uniformity score → warning log + cluster_alerts
CLUSTER_LLM_THRESHOLD = 0.85    # uniformity score → trigger cluster LLM analysis
CLUSTER_LLM_COOLDOWN = 1800     # seconds between LLM calls per host (30 min)
CLUSTER_BUFFER_MAXLEN = 500     # max sessions kept per host
CLUSTER_IP_CONCENTRATION = 0.50     # single IP fraction → UA-rotation scraper detection
CLUSTER_SUBNET_CONCENTRATION = 0.70 # single /24 subnet fraction → scraper detection
CLUSTER_SUBNET16_CONCENTRATION = 0.70 # single /16 subnet fraction → distributed scraper detection


class RebalanceLogger(ConsumerRebalanceListener):
    def __init__(self, logger, name="consumer"):
        self.logger = logger
        self.name = name

    def on_partitions_revoked(self, revoked):
        self.logger.warning("[%s] REVOKED: %s", self.name, sorted(revoked, key=lambda tp:(tp.topic,tp.partition)))

    def on_partitions_assigned(self, assigned):
        self.logger.warning("[%s] ASSIGNED: %s", self.name, sorted(assigned, key=lambda tp:(tp.topic,tp.partition)))

def log_partition_assignment(consumer, logger):
    """Logs current partitions, offsets and lag owned by this consumer."""
    try:
        assignment = consumer.assignment()  # set[TopicPartition]
        subs = consumer.subscription()  # set[str] or None
        if not assignment:
            logger.info("Assignment: ∅ (no partitions yet) | subscription=%s", list(subs) if subs else "None")
            return

        tps = sorted(list(assignment), key=lambda tp: (tp.topic, tp.partition))
        # Offsets
        ends = consumer.end_offsets(tps)  # {TopicPartition: int}
        begins = consumer.beginning_offsets(tps)  # {TopicPartition: int}
        positions = {tp: consumer.position(tp) for tp in tps}  # {TopicPartition: int or None}

        lines = []
        for tp in tps:
            pos = positions.get(tp) or 0
            end = ends.get(tp) or 0
            beg = begins.get(tp) or 0
            lag = max(0, end - pos)
            lines.append(f"{tp.topic}[{tp.partition}] pos={pos} begin={beg} end={end} lag={lag}")

        logger.info("Assignment (%d): %s | subscription=%s",
                    len(tps), "; ".join(lines), list(subs) if subs else "None")
    except Exception as e:
        logger.warning(f"Assignment logging failed: {e!r}")


def is_static_session(session: Dict[str, Any]) -> bool:
    for r in session.get("requests", []):
        if not r.get("static", False):
            return False
    return True


def _safe_shapley_report(sv, feature_names):
    """Returns empty values if shap value is missing or malformed."""
    if sv is None:
        return '', []
    try:
        return _shapley_report(sv, feature_names)
    except Exception:
        return '', []


def _shapley_report(shap_value, feature_names):
    """Extract top negative shapley contributions."""
    shapley_report = []
    min_shapley = 0
    shapley_feature = None
    for k, feature in enumerate(feature_names):
        val = shap_value.values[k]
        data_val = shap_value.data[k]
        if val < 0:
            if val < min_shapley:
                min_shapley = val
                shapley_feature = feature
            shapley_report.append(
                {
                    "name": feature,
                    "values": {"shapley": round(val, 2), "feature": round(data_val, 2)},
                }
            )
    shapley_report.sort(key=lambda x: abs(x["values"]["shapley"]), reverse=True)
    return shapley_feature, shapley_report


def _shapley_report_classifier(shap_value, feature_names):
    """Extract top negative shapley contributions."""
    shapley_report = []
    min_shapley = 0
    shapley_feature = None
    for k, feature in enumerate(feature_names):
        val = shap_value[k]
        if val < 0:
            if val < min_shapley:
                min_shapley = val
                shapley_feature = feature
            shapley_report.append(
                {
                    "name": feature,
                    "values": {"shapley": round(val, 2)},
                }
            )
    shapley_report.sort(key=lambda x: abs(x["values"]["shapley"]), reverse=True)
    return shapley_feature, shapley_report


class BaskervillehallPredictor(object):
    def __init__(
            self,
            topic_sessions="BASKERVILLEHALL_SESSIONS",
            group_id='predict_pipeline',
            topic_commands="banjax_command_topic",
            topic_commands_output="banjax_command_topic",
            topic_reports="banjax_report_topic",
            kafka_connection=None,
            kafka_connection_output=None,
            s3_connection=None,
            s3_path="/",
            datetime_format="%Y-%m-%d %H:%M:%S",
            white_list_refresh_in_minutes=5,
            model_reload_in_minutes=10,
            max_models=10000,
            min_session_duration=20,
            min_number_of_requests=2,
            num_offences_for_difficult_challenge=3,
            batch_size=100,
            worker_chunk_size=1000,
            kafka_poll_timeout_ms=5000,
            max_poll_interval_ms=600000,
            fetch_max_wait_ms=2000,
            fetch_min_bytes=1048576,
            lag_high_threshold=10000,
            lag_moderate_threshold=5000,
            pending_ttl=30,
            maxsize_pending=10000000,
            n_jobs_predict=-1,
            logger=None,
            deflect_config_url=None,
            deflect_config_auth=None,
            ip_whitelist_url=None,
            ip_whitelist_auth=None,
            global_allowlist_url=None,
            global_allowlist_auth=None,
            origin_ips_url=None,
            origin_ips_auth=None,
            white_list_refresh_period=5,
            bad_bot_challenge=True,
            debug_ip=None,
            use_shapley=True,
            postgres_connection=None,
            postgres_refresh_period_in_seconds=180,
            sensitivity_factor=0.05,
            max_sessions_for_ip=10,
            maz_size_ip_sessions=100000,
            ip_sessions_ttl_in_minutes=30,
            max_requests_in_command=20,
            bot_score_threshold=0.5,
            challenge_scrapers=True,
            block_commercial_crawlers=True,
            rate_limit_hits=20,
            rate_limit_interval=60,
            rate_limit_expiration=300,
            use_rate_limit=True,
            dnet_partition_map=None,
            print_log_in_command=True,
            use_baskerville_score=True,
            verbose_classifier=False,
            hostname='localhost',
            attack_min_challenge_count=50,
            attack_min_spike_ratio=4.0,
            attack_aggressive_spike_ratio=6.0,
            attack_extreme_spike_ratio=15.0,
            attack_response_mode=True,
            session_llm_enabled=True,
            ollama_url='http://localhost:11434',
            llm_model='qwen2.5:7b',
            session_llm_score_min=20,
            session_llm_score_max=80,
            session_llm_min_requests=5,
            session_llm_queue_size=200,
            session_llm_provider='ollama',
            openai_api_key='',
    ):
        super().__init__()

        if s3_connection is None:
            s3_connection = {}
        if postgres_connection is None:
            postgres_connection = {}
        if kafka_connection is None:
            kafka_connection = {"bootstrap_servers": "localhost:9092"}
        if kafka_connection_output is None:
            kafka_connection_output = {"bootstrap_servers": "localhost:9092"}

        self.topic_sessions = topic_sessions
        self.group_id = group_id
        self.topic_commands = topic_commands
        self.kafka_connection = kafka_connection
        self.kafka_connection_output = kafka_connection_output
        self.s3_connection = s3_connection
        self.postgres_connection = postgres_connection
        self.s3_path = s3_path
        self.hostname = hostname
        self.min_session_duration = min_session_duration
        self.min_number_of_requests = min_number_of_requests
        self.white_list_refresh_in_minutes = white_list_refresh_in_minutes
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.model_reload_in_minutes = model_reload_in_minutes
        self.max_models = max_models
        self.pending_ttl = pending_ttl
        self.topic_reports = topic_reports
        self.maxsize_pending = maxsize_pending
        self.batch_size = batch_size
        self.worker_chunk_size = worker_chunk_size
        self.kafka_poll_timeout_ms = kafka_poll_timeout_ms
        self.max_poll_interval_ms = max_poll_interval_ms
        self.fetch_max_wait_ms = fetch_max_wait_ms
        self.fetch_min_bytes = fetch_min_bytes
        self.lag_high_threshold = lag_high_threshold
        self.lag_moderate_threshold = lag_moderate_threshold
        self.current_lag = 0
        self.adaptive_processing = True
        self.date_time_format = datetime_format
        self.debug_ip = debug_ip
        self.n_jobs_predict = n_jobs_predict
        self.num_offences_for_difficult_challenge = num_offences_for_difficult_challenge
        self.deflect_config_url = deflect_config_url
        self.deflect_config_auth = deflect_config_auth
        self.white_list_refresh_period = white_list_refresh_period
        self.bad_bot_challenge = bad_bot_challenge
        self.use_shapley = use_shapley
        self.max_sessions_for_ip = max_sessions_for_ip
        self.maxsize_ip_sessions = maz_size_ip_sessions
        self.ip_sessions_ttl_in_minutes = ip_sessions_ttl_in_minutes
        self.max_requests_in_command = max_requests_in_command
        self.bot_score_threshold = bot_score_threshold
        self.challenge_scrapers = challenge_scrapers
        self.block_commercial_crawlers = block_commercial_crawlers
        self.rate_limit_hits = rate_limit_hits,
        self.rate_limit_interval = rate_limit_interval,
        self.rate_limit_expiration = rate_limit_expiration
        self.use_rate_limit = use_rate_limit
        self.topic_commands_output = topic_commands_output
        self.topic_commands_output = topic_commands_output
        self.dnet_partition_map = dnet_partition_map
        self.print_log_in_command = print_log_in_command
        self.use_baskerville_score = use_baskerville_score
        self.verbose_classifier = verbose_classifier

        if deflect_config_url is None or len(deflect_config_url) == 0:
            self.settings = SettingsPostgres(
                refresh_period_in_seconds=postgres_refresh_period_in_seconds,
                postgres_connection=postgres_connection,
            )
        else:
            self.settings = SettingsDeflectAPI(
                url=self.deflect_config_url,
                auth=self.deflect_config_auth,
                ip_whitelist_url=ip_whitelist_url,
                ip_whitelist_auth=ip_whitelist_auth,
                global_allowlist_url=global_allowlist_url,
                global_allowlist_auth=global_allowlist_auth,
                origin_ips_url=origin_ips_url,
                origin_ips_auth=origin_ips_auth,
                logger=self.logger,
                refresh_period_in_seconds=60 * self.white_list_refresh_period,
            )
        self.sensitivity_factor = sensitivity_factor

        # Initialize models directly in main thread
        self.models_if = ModelStorage(
            s3_connection,
            s3_path,
            reload_in_minutes=model_reload_in_minutes,
            logger=self.logger,
        )
        self.models_ae = ModelStorage(
            s3_connection,
            f"{s3_path}_autoencoder3",
            reload_in_minutes=model_reload_in_minutes,
            logger=self.logger,
        )
        self.models_classifier = ModelStorage(
            s3_connection,
            f"{s3_path}_classifier",
            reload_in_minutes=model_reload_in_minutes,
            logger=self.logger,
        )

        self._attack_response_mode: bool = attack_response_mode
        self._attack_response_hosts: dict = {}  # host → spike_ratio
        self._first_responder_actions: dict = {}  # host → {'action': str, 'target': set[str]}
        self._last_incident_check: float = 0.0
        self._incident_check_interval: int = 30  # seconds
        self._attack_min_challenge_count: int = attack_min_challenge_count
        self._attack_min_spike_ratio: float = attack_min_spike_ratio
        self._attack_aggressive_spike_ratio: float = attack_aggressive_spike_ratio
        self._attack_extreme_spike_ratio: float = attack_extreme_spike_ratio

        self._session_llm_enabled: bool = session_llm_enabled
        self._ollama_url: str = ollama_url
        self._llm_model: str = llm_model
        self._session_llm_score_min: int = session_llm_score_min
        self._session_llm_score_max: int = session_llm_score_max
        self._session_llm_min_requests: int = session_llm_min_requests
        self._session_llm_queue_size: int = session_llm_queue_size
        self._session_llm_provider: str = session_llm_provider  # 'ollama' or 'openai'
        self._openai_api_key: str = openai_api_key
        self._session_llm_queue: Queue = None   # initialized in run()
        self._session_llm_cache: TTLCache = None  # ip → verdict, initialized in run()
        self._cluster_llm_queue: Queue = None   # initialized in run()

    def _refresh_attack_response(self):
        """Poll Postgres incidents table to discover hosts currently under DDoS attack."""
        if not self._attack_response_mode:
            return
        if not self.postgres_connection:
            return
        now = time_module.time()
        if now - self._last_incident_check < self._incident_check_interval:
            return
        self._last_incident_check = now
        try:
            conn = psycopg2.connect(**self.postgres_connection)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT host, MAX(spike_ratio) FROM incidents "
                    "WHERE ended_at IS NULL "
                    "AND started_at > NOW() - INTERVAL '30 minutes' "
                    "AND challenge_count >= %s "
                    "AND spike_ratio >= %s "
                    "GROUP BY host",
                    (self._attack_min_challenge_count, self._attack_min_spike_ratio),
                )
                rows = cur.fetchall()
            new_hosts = {row[0]: float(row[1]) for row in rows}
            if new_hosts != self._attack_response_hosts:
                if new_hosts:
                    for h, ratio in new_hosts.items():
                        if ratio >= self._attack_extreme_spike_ratio:
                            level = "EXTREME"
                        elif ratio >= self._attack_aggressive_spike_ratio:
                            level = "AGGRESSIVE"
                        else:
                            level = "MODERATE"
                        self.logger.warning(
                            f"AttackResponse mode [{level}] host={h} spike_ratio={ratio:.1f}"
                        )
                else:
                    self.logger.info("AttackResponse mode: no active incidents")
            self._attack_response_hosts = new_hosts

            # Poll first_responder_actions for active LLM-issued blocks
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT host, action, target FROM first_responder_actions "
                    "WHERE expires_at > NOW() "
                    "ORDER BY created_at DESC"
                )
                action_rows = cur.fetchall()
                cur.execute(
                    "UPDATE first_responder_actions SET applied = TRUE "
                    "WHERE expires_at > NOW() AND applied = FALSE"
                )
            conn.commit()
            conn.close()

            new_responder = {}
            for host, action, target_str in action_rows:
                if host not in new_responder:  # keep most recent per host
                    if action not in ('block_asn', 'block_country', 'block_ua'):
                        continue
                    targets = {t.strip() for t in target_str.split('|') if t.strip()}
                    new_responder[host] = {'action': action, 'target': targets}

            if new_responder != self._first_responder_actions:
                if new_responder:
                    for h, ra in new_responder.items():
                        self.logger.warning(
                            f"FirstResponder action active: host={h} "
                            f"action={ra['action']} target={ra['target']}"
                        )
                else:
                    self.logger.info("FirstResponder: no active actions")
            self._first_responder_actions = new_responder

        except Exception:
            self.logger.exception("Failed to refresh incident state from Postgres (non-fatal)")

    # URL prefixes that indicate a logged-in CMS/admin user
    _ADMIN_URL_PATTERNS = (
        '/admin', '/wp-admin', '/wp-json/wp/v2',
        '/media-library', '/node/add', '/node/edit',
        '/user/login', '/user/register', '/user/password',
        '/dashboard', '/manage', '/backend',
        '/edit/', '/update/', '/delete/',
        '/upload', '/filemanager',
    )

    def _build_session_llm_prompt(self, session: dict, host: str) -> str:
        requests_raw = sorted(
            session.get('requests', []),
            key=lambda r: r.get('ts', ''),
        )[:10]

        lines = []
        api_count = 0
        post_count = 0
        admin_count = 0
        rsc_count = 0       # Next.js React Server Components navigation
        wp_ajax_count = 0   # WordPress browser-side AJAX (GDPR, comments, tracking)
        ajax_count = 0      # Generic AJAX/XHR (getFacet, inline JSON)
        method_counts: dict = {}

        for r in requests_raw:
            url = r.get('url', '/')
            query = r.get('query', '') or ''
            ctype = r.get('type', 'text/html').split(';')[0].strip()
            method = r.get('method', 'GET')
            full_url = f"{url}?{query}" if query else url
            q_str = f'?{query[:80]}' if query else ''
            lines.append(f"  {method} {url}{q_str}  [{ctype}]")
            method_counts[method] = method_counts.get(method, 0) + 1
            if '/api/' in url or url.startswith('/api'):
                api_count += 1
            if method == 'POST':
                post_count += 1
            if any(pat in url for pat in self._ADMIN_URL_PATTERNS):
                admin_count += 1
            # Next.js RSC: _rsc= query param, text/x-component, or /_next/ assets
            if '_rsc=' in query or ctype == 'text/x-component' or '/_next/' in url:
                rsc_count += 1
            # WordPress browser-side AJAX endpoints (called by JS, not admins)
            if 'admin-ajax.php' in url or '/wp-json/' in url:
                wp_ajax_count += 1
            # Generic AJAX patterns (getFacet, XHR JSON endpoints)
            if 'getFacet' in url or 'ajax' in url.lower() or ctype in ('application/json', 'text/x-component'):
                ajax_count += 1

        requests_text = '\n'.join(lines)
        duration = session.get('duration', 0)
        num_req = len(session.get('requests', []))
        ua = session.get('ua', '')[:120]
        method_summary = ', '.join(f"{m}:{c}" for m, c in sorted(method_counts.items()))
        n = len(requests_raw) or 1

        # Choose context in priority order
        if rsc_count >= n * 0.5:
            site_context = (
                "CONTEXT: This is a Next.js site using React Server Components (RSC). "
                "When a user navigates between pages in a Next.js SPA, the browser fetches "
                "'text/x-component' resources with '?_rsc=...' query parameters — these are "
                "server-rendered component payloads, NOT bot traffic. "
                "/_next/image URLs are Next.js image optimization requests — these appear "
                "whenever a Next.js page loads images, and are a strong signal of a real browser. "
                "A mix of /_next/image requests + RSC fetches is the typical pattern of a human "
                "browsing a Next.js site: images load first, then the user navigates via RSC. "
                "Different _rsc tokens for the same URL are normal (cache-busting). "
                "Label as bot only if: the UA is a known crawler, or URLs show clear "
                "sitemap/enumeration patterns (sequential IDs, /sitemap.xml, /robots.txt)."
            )
        elif wp_ajax_count >= 2 or (wp_ajax_count > 0 and post_count > 0):
            site_context = (
                "CONTEXT: This WordPress site uses REST API and AJAX endpoints that are "
                "called by browser-side JavaScript — NOT necessarily by admin users. "
                "Common browser-triggered calls: wp-json/complianz (GDPR consent), "
                "wp-json/<plugin>/* (donation/payment tracking, newsletter, etc.), "
                "admin-ajax.php (comments, search, analytics, any plugin). "
                "These appear in sessions of ORDINARY visitors when plugins run JS. "
                "Presence of UTM/tracking parameters (utm_source, gclid, fbclid) in "
                "any URL strongly indicates a real visitor from ads or social media. "
                "Label as bot only if the UA is a known crawler or URLs show clear "
                "reconnaissance/data-harvesting patterns."
            )
        elif ajax_count >= n * 0.5 and post_count == 0:
            site_context = (
                "CONTEXT: This session consists mainly of AJAX/XHR requests "
                "(faceted search, inline data endpoints, JSON APIs). "
                "Modern web applications fire many background XHR calls as the user "
                "interacts with filters, search facets, or dynamic content — this is "
                "normal human behavior in archive, search, or dashboard UIs. "
                "Label as bot only if URLs show sequential numeric ID enumeration, "
                "the UA is a known crawler, or requests target sensitive paths "
                "(/.env, /.git, /phpinfo, /server-status)."
            )
        elif api_count >= n * 0.6:
            site_context = (
                "CONTEXT: This site exposes a REST/ActivityPub API. "
                "Mobile apps, desktop clients, and browser SPAs call these endpoints on behalf of real users. "
                "API polling (notifications, timelines, feed updates) by a legitimate client User-Agent is HUMAN behavior. "
                "Label as bot only if: UA is a known crawler/script, URLs show sequential ID enumeration, "
                "or the pattern matches sitemap/data harvesting."
            )
        elif admin_count > 0 or post_count >= n * 0.3:
            site_context = (
                "CONTEXT: This session contains POST requests and/or CMS admin/editor URLs. "
                "Bots almost never send POST requests — they scrape content with GET. "
                "POST-heavy sessions typically indicate a logged-in user submitting forms, "
                "editing content in a CMS (Drupal, WordPress, etc.), or uploading media. "
                "Admin paths (/admin/, /media-library/, /node/add/, /edit/, /upload/) "
                "strongly indicate a legitimate site editor or administrator. "
                "Label as bot only if the UA is a known crawler or the pattern clearly shows "
                "automated data extraction (no POST, sequential IDs, sitemap crawling)."
            )
        else:
            site_context = (
                "CONTEXT: This site serves web pages. "
                "Human browsing ALWAYS mixes HTML pages with CSS/JS/image assets — a real browser "
                "loads stylesheets, scripts, and images alongside each HTML page. "
                "CRITICAL BOT SIGNALS (any one is sufficient): "
                "(1) Session contains ONLY [text/html] responses with NO [image/...], [text/css], "
                "[application/javascript] responses — real browsers always load assets. "
                "(2) Sequential numeric page IDs (/?p=12345, /?p=12346, ...). "
                "(3) Pattern of /?p=ID followed by /date/category/ID/ for the same IDs — this is "
                "a WordPress bot that fetches by ID then follows the canonical URL, NOT human navigation. "
                "(4) Sitemap or feed crawling (/sitemap.xml, /feed, /rss/). "
                "(5) Non-browser or empty UA. "
                "Topic-based URLs and varied article paths do NOT indicate human traffic on their own — "
                "bots crawl topic-organized sites too. The key differentiator is asset loading."
            )

        return f"""Analyze this web session on site: {host}
Session: {num_req} requests over {duration:.0f}s ({method_summary})
User-Agent: {ua}

{site_context}

Request sequence:
{requests_text}

Reply JSON only, no markdown:
{{"label": "human" or "bot", "confidence": 0.0-1.0, "reasoning": "2-3 sentences"}}"""

    def _call_llm(self, prompt: str, model: str, timeout: int = 30) -> str:
        """Call LLM API (Anthropic or Ollama) and return raw text content."""
        if self._session_llm_provider == 'anthropic':
            response = http_requests.post(
                'https://api.anthropic.com/v1/messages',
                headers={
                    'x-api-key': self._openai_api_key,
                    'anthropic-version': '2023-06-01',
                    'content-type': 'application/json',
                },
                json={
                    'model': model,
                    'max_tokens': 1024,
                    'temperature': 0.0,
                    'messages': [{'role': 'user', 'content': prompt}],
                },
                timeout=timeout,
            )
            resp_json = response.json()
            if 'content' not in resp_json:
                raise ValueError(f"Anthropic API error: {resp_json}")
            return resp_json['content'][0]['text'].strip()
        else:
            response = http_requests.post(
                f'{self._ollama_url}/v1/chat/completions',
                headers={},
                json={
                    'model': model,
                    'messages': [{'role': 'user', 'content': prompt}],
                    'temperature': 0.0,
                },
                timeout=timeout,
            )
            return response.json()['choices'][0]['message']['content'].strip()

    def _session_llm_score(self, session: dict, host: str) -> dict | None:
        try:
            prompt = self._build_session_llm_prompt(session, host)
            content = self._call_llm(prompt, model=self._llm_model, timeout=30)
            # Strip markdown code blocks if model wraps JSON in them
            if content.startswith('```'):
                content = content.split('```')[1]
                if content.startswith('json'):
                    content = content[4:]
            return json.loads(content.strip())
        except Exception:
            self.logger.exception(
                f"[SESSION_LLM] Error for ip={session.get('ip')} host={host}"
            )
            return None

    def _save_session_llm_verdict(self, session: dict, host: str, baskerville_score: int, verdict: dict):
        requests_raw = sorted(session.get('requests', []), key=lambda r: r.get('ts', ''))[:20]
        url_sequence = '\n'.join(
            f"{r.get('method','GET')} {r.get('url','/')}"
            + (f"?{r.get('query','')}" if r.get('query') else '')
            for r in requests_raw
        )
        try:
            conn = psycopg2.connect(**self.postgres_connection)
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO session_llm_verdicts
                       (host, ip, session_id, baskerville_score, num_requests, duration,
                        ua, llm_label, llm_confidence, llm_reasoning, url_sequence)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        host,
                        session.get('ip', ''),
                        session.get('session_id', ''),
                        baskerville_score,
                        len(session.get('requests', [])),
                        session.get('duration', 0),
                        session.get('ua', '')[:500],
                        verdict.get('label', ''),
                        float(verdict.get('confidence', 0.0)),
                        verdict.get('reasoning', ''),
                        url_sequence[:2000],
                    ),
                )
            conn.commit()
            conn.close()
        except Exception:
            self.logger.exception(f"[SESSION_LLM] Failed to save verdict for ip={session.get('ip')} host={host}")

    def _llm_session_worker(self):
        self.logger.info("[SESSION_LLM] Worker started, model=%s", self._llm_model)
        producer = KafkaProducer(**self.kafka_connection)
        producer_output = KafkaProducer(**self.kafka_connection_output)
        pending_challenge = TTLCache(maxsize=50000, ttl=self.pending_ttl)

        while True:
            try:
                item = self._session_llm_queue.get(timeout=30)
            except Exception:
                continue

            session = item['session']
            host = item['host']
            baskerville_score = item['baskerville_score']
            dnet = item.get('dnet', '-')
            ip = session.get('ip', '')

            verdict = self._session_llm_score(session, host)
            if verdict is None:
                continue

            label = verdict.get('label', '')
            confidence = verdict.get('confidence', 0.0)
            reasoning = verdict.get('reasoning', '')

            # Cache verdict by IP for cluster analysis
            if self._session_llm_cache is not None:
                self._session_llm_cache[ip] = verdict

            self.logger.info(
                f"[SESSION_LLM] host={host} ip={ip} baskerville_score={baskerville_score} "
                f"label={label} confidence={confidence:.2f} reasoning={reasoning}"
            )

            if self.postgres_connection:
                self._save_session_llm_verdict(session, host, baskerville_score, verdict)

            if label == 'bot' and confidence >= 0.85 and ip not in pending_challenge:
                pending_challenge[ip] = True
                command = 'challenge_ip' if confidence >= 0.9 else 'challenge_session'
                scraper_name = session.get('scraper_name', detect_scraper(session.get('ua')))
                payload = self.create_command(
                    command_name=command,
                    session=session,
                    meta=f"session_llm [{command}] confidence={confidence:.2f}",
                    prediction_if=False,
                    score_if=0.0,
                    shapley_if='',
                    shapley_feature_if='',
                    prediction_ae=False,
                    score_ae=0.0,
                    shapley_ae='',
                    shapley_feature_ae='',
                    difficulty=0,
                    scraper_name=scraper_name,
                    threshold_ae=0.0,
                    baskerville_score=baskerville_score,
                )
                self.send(producer, producer_output, payload, key=host, dnet=dnet)
                producer.flush()
                producer_output.flush()
                self.logger.warning(
                    f"[SESSION_LLM] {command} ip={ip} host={host} "
                    f"confidence={confidence:.2f} reasoning={reasoning}"
                )

    # ------------------------------------------------------------------
    # Cluster LLM analysis
    # ------------------------------------------------------------------

    def _build_cluster_llm_prompt(self, host: str, sessions: list, score: float) -> str:
        n = len(sessions)
        uas = [s['ua'] for s in sessions if s.get('ua')]
        fps = [s['fingerprint'] for s in sessions if s.get('fingerprint')]
        cvs = [s['interval_cv'] for s in sessions if s.get('interval_cv') is not None]
        all_paths = [p for s in sessions for p in s.get('url_paths', [])]

        ua_counts = Counter(uas)
        fp_counts = Counter(fps)

        ua_lines = '\n'.join(
            f'  "{ua[:80]}" — {cnt} sessions ({cnt * 100 // n}%)'
            for ua, cnt in ua_counts.most_common(5)
        ) or '  (none)'
        fp_lines = '\n'.join(
            f'  {fp} — {cnt} sessions ({cnt * 100 // n}%)'
            for fp, cnt in fp_counts.most_common(5)
        ) or '  (none)'
        ip_list = ', '.join({s['ip'] for s in sessions if s.get('ip')})

        avg_cv = f'{statistics.mean(cvs):.3f}' if cvs else 'n/a'
        top_path = Counter(all_paths).most_common(1)
        top_path_str = f'{top_path[0][0]} ({top_path[0][1] * 100 // max(len(all_paths), 1)}%)' if top_path else 'n/a'

        # Session LLM verdicts (if available)
        llm_lines = ''
        if self._session_llm_cache is not None:
            verdicts = [(s['ip'], self._session_llm_cache.get(s['ip'])) for s in sessions]
            found = [(ip, v) for ip, v in verdicts if v is not None]
            if found:
                bot_count = sum(1 for _, v in found if v.get('label') == 'bot')
                llm_lines = (
                    f'\nSESSION LLM VERDICTS ({len(found)}/{n} sessions analyzed):\n'
                    f'  bot={bot_count}/{len(found)}, '
                    f'human={len(found) - bot_count}/{len(found)}\n'
                    + '\n'.join(
                        f'  ip={ip} label={v.get("label")} confidence={v.get("confidence", 0):.2f}'
                        for ip, v in found[:5]
                    )
                )

        return (
            f'You are a DDoS detection expert. Analyze this cluster of web sessions.\n\n'
            f'Host: {host}\n'
            f'Window: last {CLUSTER_CHECK_INTERVAL // 60} minutes\n'
            f'Sessions: {n}\n'
            f'Uniformity score: {score:.2f} (0=organic, 1=fully coordinated bot)\n\n'
            f'CLUSTER SIGNALS:\n'
            f'  fingerprint diversity: {len(set(fps))}/{len(fps) or 1:.0f} unique '
            f'(1 = same HTTP library across all sessions)\n'
            f'  UA diversity: {len(set(uas))}/{len(uas) or 1:.0f} unique\n'
            f'  avg interval_cv: {avg_cv} (0.0 = perfect bot timer, >0.5 = human-like)\n'
            f'  top URL pattern: {top_path_str}\n\n'
            f'TOP USER AGENTS:\n{ua_lines}\n\n'
            f'TOP FINGERPRINTS (UA+TLS cipher order+Accept-Language hash):\n{fp_lines}\n\n'
            f'IP LIST: {ip_list}\n'
            f'{llm_lines}\n\n'
            f'Is this a coordinated bot attack or organic traffic?\n'
            f'Respond with JSON only:\n'
            f'{{"verdict": "attack" | "benign" | "uncertain", '
            f'"confidence": "high" | "medium" | "low", '
            f'"reasoning": "1-2 sentences"}}'
        )

    def _build_block_criteria(self, sessions: list, score: float) -> list:
        """Derive blocking criteria from cluster features. Each criterion is self-contained.

        Each criterion has an 'action' field: 'block' or 'challenge'.
        Conservative by design — challenge on ambiguous signals, block only on very strong evidence.
        """
        criteria = []

        # Fingerprint — always challenge, never block.
        # A fingerprint (UA+TLS cipher order+Accept-Language) can match many legitimate users
        # on the same browser version. Even 100% concentration is not safe to block.
        fps = [s['fingerprint'] for s in sessions if s.get('fingerprint')]
        if fps:
            for fp, count in Counter(fps).most_common(3):
                pct = count / len(fps) * 100
                if pct >= 30:
                    fp_ips = list(dict.fromkeys(s['ip'] for s in sessions if s.get('fingerprint') == fp and s.get('ip')))
                    criteria.append({
                        'type': 'fingerprint',
                        'action': 'challenge',
                        'value': fp,
                        'count': count,
                        'pct': round(pct, 1),
                        'ips': fp_ips,
                        'confidence': 'high' if pct >= 70 else 'medium',
                    })

        # IP list — split by interval_cv strength.
        cvs_by_ip = {s['ip']: s.get('interval_cv') for s in sessions if s.get('ip')}

        # Hard block: cv < 0.05 — near-perfect bot timer. Humans cannot maintain this regularity.
        hard_bot_ips = [ip for ip, cv in cvs_by_ip.items() if cv is not None and cv < 0.05]
        if hard_bot_ips:
            criteria.append({
                'type': 'ip_list',
                'action': 'block',
                'ips': hard_bot_ips,
                'reason': 'interval_cv<0.05 (near-perfect bot timer)',
                'confidence': 'high',
            })

        # Soft challenge: cv 0.05–0.10 — suspicious regularity but not conclusive.
        soft_bot_ips = [ip for ip, cv in cvs_by_ip.items() if cv is not None and 0.05 <= cv < 0.10]
        if soft_bot_ips:
            criteria.append({
                'type': 'ip_list',
                'action': 'challenge',
                'ips': soft_bot_ips,
                'reason': 'interval_cv 0.05–0.10 (suspicious regularity)',
                'confidence': 'medium',
            })

        # IP concentration — if a single IP or /24 subnet generates the majority of sessions,
        # block it directly regardless of UA/fingerprint diversity. This catches UA-rotation
        # scrapers that evade uniformity scoring by spoofing many different browser versions.
        ip_list_all = [s['ip'] for s in sessions if s.get('ip')]
        if ip_list_all:
            ip_counts = Counter(ip_list_all)
            top_ip, top_ip_count = ip_counts.most_common(1)[0]
            ip_concentration = top_ip_count / len(ip_list_all)
            if ip_concentration >= CLUSTER_IP_CONCENTRATION:
                # Single IP dominates → block it directly
                if top_ip not in hard_bot_ips and top_ip not in soft_bot_ips:
                    criteria.append({
                        'type': 'ip_list',
                        'action': 'block',
                        'ips': [top_ip],
                        'reason': f'ip_concentration={ip_concentration:.0%} (single IP dominates cluster)',
                        'confidence': 'high',
                    })
            else:
                subnet_counts = Counter('.'.join(ip.split('.')[:3]) for ip in ip_list_all)
                top_subnet, top_subnet_count = subnet_counts.most_common(1)[0]
                subnet_concentration = top_subnet_count / len(ip_list_all)
                if subnet_concentration >= CLUSTER_SUBNET_CONCENTRATION:
                    # /24 subnet dominates → block all IPs from it not already blocked
                    subnet_ips = [ip for ip in ip_counts if ip.startswith(top_subnet + '.')
                                  and ip not in hard_bot_ips and ip not in soft_bot_ips]
                    if subnet_ips:
                        criteria.append({
                            'type': 'ip_list',
                            'action': 'block',
                            'ips': subnet_ips,
                            'reason': f'subnet_concentration={subnet_concentration:.0%} ({top_subnet}.x dominates cluster)',
                            'confidence': 'high',
                        })
                else:
                    # /16 subnet — catches distributed scrapers rotating across multiple /24s
                    subnet16_counts = Counter('.'.join(ip.split('.')[:2]) for ip in ip_list_all)
                    top_subnet16, top_subnet16_count = subnet16_counts.most_common(1)[0]
                    subnet16_concentration = top_subnet16_count / len(ip_list_all)
                    if subnet16_concentration >= CLUSTER_SUBNET16_CONCENTRATION:
                        subnet16_ips = [ip for ip in ip_counts if ip.startswith(top_subnet16 + '.')
                                        and ip not in hard_bot_ips and ip not in soft_bot_ips]
                        if subnet16_ips:
                            criteria.append({
                                'type': 'ip_list',
                                'action': 'block',
                                'ips': subnet16_ips,
                                'reason': f'subnet16_concentration={subnet16_concentration:.0%} ({top_subnet16}.x.x dominates cluster)',
                                'confidence': 'medium',
                            })

        # UA — two tiers:
        # - Scripted UAs (python-requests, curl, etc.): block current IPs + block_ua for future
        # - Browser UAs: challenge current IPs only (UA not unique, can't block future sessions)
        _SCRIPTED_PATTERNS = (
            'python-requests', 'python-urllib', 'python/',
            'curl/', 'wget/', 'httpie/',
            'go-http-client', 'java/', 'okhttp', 'apache-httpclient',
            'headlesschrome', 'phantomjs', 'puppeteer',
        )
        uas = [s['ua'] for s in sessions if s.get('ua')]
        if uas:
            for ua, count in Counter(uas).most_common(2):
                pct = count / len(uas) * 100
                if pct >= 50 and count >= 5:
                    ua_ips = list(dict.fromkeys(s['ip'] for s in sessions if s.get('ua') == ua and s.get('ip')))
                    is_scripted = any(p in ua.lower() for p in _SCRIPTED_PATTERNS)
                    criteria.append({
                        'type': 'ua_exact',
                        'action': 'block' if is_scripted else 'challenge',
                        'value': ua,
                        'ips': ua_ips,
                        'count': count,
                        'pct': round(pct, 1),
                        'confidence': 'high' if is_scripted else 'medium',
                        'is_scripted': is_scripted,
                    })

        return criteria

    @staticmethod
    def _cluster_criteria_signature(block_criteria: list) -> frozenset:
        """Stable signature of block_criteria for change detection.

        Captures fingerprint values, ua_exact values, and concentration-blocked IPs.
        Regular ip_list IPs (interval_cv) are excluded — they change every window
        but represent the same attack. Concentration IPs are included because a new
        dominant IP means a genuinely different scraper node.
        """
        fingerprints = frozenset(
            c['value'] for c in block_criteria if c.get('type') == 'fingerprint'
        )
        uas = frozenset(
            c['value'] for c in block_criteria if c.get('type') == 'ua_exact'
        )
        # Include IPs blocked by concentration rule (not interval_cv) so that a new
        # dominant IP triggers a fresh incident.
        concentration_ips = frozenset(
            ip
            for c in block_criteria
            if c.get('type') == 'ip_list' and 'concentration' in c.get('reason', '')
            for ip in c.get('ips', [])
        )
        return frozenset([('fp', fingerprints), ('ua', uas), ('conc_ips', concentration_ips)])

    def _save_cluster_incident(self, host: str, score: float, sessions: list,
                               block_criteria: list, llm_reasoning: str,
                               survey_country: str = ''):
        n = len(sessions)
        try:
            conn = psycopg2.connect(**self.postgres_connection)
            with conn.cursor() as cur:
                # Dedup: skip if the most recent cluster_analysis incident for this host
                # (within last 4 hours) has identical block_criteria signature —
                # same fingerprints + same UA. ip_list IPs are excluded from signature
                # because individual IPs change each window while the attack stays the same.
                # Only create a new incident if fingerprint or UA changed (evolved attack).
                cur.execute(
                    """SELECT block_criteria FROM incidents
                       WHERE host = %s AND source = 'cluster_analysis'
                         AND started_at > NOW() - INTERVAL '4 hours'
                       ORDER BY started_at DESC
                       LIMIT 1""",
                    (host,),
                )
                row = cur.fetchone()
                if row and row[0]:
                    prev_criteria = row[0] if isinstance(row[0], list) else json.loads(row[0])
                    prev_sig = self._cluster_criteria_signature(prev_criteria)
                    new_sig = self._cluster_criteria_signature(block_criteria)
                    if prev_sig == new_sig:
                        self.logger.info(
                            f"[CLUSTER_LLM] Skipping duplicate incident for host={host} "
                            f"(block_criteria unchanged)"
                        )
                        conn.close()
                        return
                    self.logger.info(
                        f"[CLUSTER_LLM] Attack evolved for host={host}, creating new incident"
                    )

                cur.execute(
                    """INSERT INTO incidents
                       (host, started_at, ended_at, challenge_count, baseline_avg, spike_ratio,
                        command, window_seconds, source, block_criteria, narrative, survey_country)
                       VALUES (%s, NOW(), NOW(), 0, 0, %s,
                               'cluster_analysis', %s, 'cluster_analysis', %s, %s, %s)
                       RETURNING id""",
                    (
                        host,
                        round(score, 3),
                        CLUSTER_CHECK_INTERVAL,
                        json.dumps(block_criteria),
                        f"[CLUSTER] uniformity_score={score:.2f} sessions={n}\n{llm_reasoning}",
                        survey_country or None,
                    ),
                )
                incident_id = cur.fetchone()[0]

                # Insert IPs from block_criteria into incident_ips so botnet overlap
                # detection (BOTNET_OVERLAP_SQL) can match against past incidents.
                all_ips = set()
                for c in block_criteria:
                    for ip in c.get('ips', []):
                        all_ips.add(ip)
                if all_ips:
                    cur.executemany(
                        "INSERT INTO incident_ips (incident_id, ip) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        [(incident_id, ip) for ip in all_ips],
                    )

            conn.commit()
            conn.close()
            self.logger.warning(
                f"[CLUSTER_LLM] Incident created: host={host} incident_id={incident_id} "
                f"score={score:.2f} criteria={len(block_criteria)}"
            )
        except Exception:
            self.logger.exception(f"[CLUSTER_LLM] Failed to save incident for host={host}")

    def _cluster_llm_worker(self):
        self.logger.info("[CLUSTER_LLM] Worker started")
        while True:
            try:
                item = self._cluster_llm_queue.get(timeout=60)
            except Exception:
                continue

            host = item['host']
            sessions = item['sessions']
            score = item['score']
            survey_country = item.get('survey_country', '')

            try:
                prompt = self._build_cluster_llm_prompt(host, sessions, score)
                content = self._call_llm(prompt, model=self._llm_model, timeout=30)
                if content.startswith('```'):
                    content = content.split('```')[1]
                    if content.startswith('json'):
                        content = content[4:]
                verdict = json.loads(content.strip())
            except Exception:
                self.logger.exception(f"[CLUSTER_LLM] Error for host={host}")
                continue

            v = verdict.get('verdict', 'uncertain')
            confidence = verdict.get('confidence', 'low')
            reasoning = verdict.get('reasoning', '')

            self.logger.warning(
                f"[CLUSTER_LLM] host={host} score={score:.2f} verdict={v} "
                f"confidence={confidence} reasoning={reasoning}"
            )

            if v == 'attack' and confidence in ('high', 'medium') and self.postgres_connection:
                block_criteria = self._build_block_criteria(sessions, score)
                if block_criteria:
                    self._save_cluster_incident(host, score, sessions, block_criteria, reasoning, survey_country)

    # ------------------------------------------------------------------
    # Cluster analysis — uniformity scoring
    # ------------------------------------------------------------------

    def _extract_cluster_features(self, session: dict) -> dict:
        """Extract lightweight per-session data for cluster_buffer."""
        requests = session.get('requests', [])
        sorted_reqs = sorted(requests, key=lambda x: x.get('ts', ''))[:20]

        url_paths = [r.get('url', '/') for r in sorted_reqs[:10]]

        # Compute inter-request intervals from raw timestamps
        intervals = []
        prev_ts = None
        for r in sorted_reqs:
            ts = r.get('ts')
            if prev_ts is not None and ts is not None:
                try:
                    ts_dt = datetime.strptime(ts, self.date_time_format) if isinstance(ts, str) else ts
                    prev_dt = datetime.strptime(prev_ts, self.date_time_format) if isinstance(prev_ts, str) else prev_ts
                    delta = (ts_dt - prev_dt).total_seconds()
                    if 0 < delta < 300:
                        intervals.append(delta)
                except Exception:
                    pass
            prev_ts = ts

        # interval_cv: coefficient of variation of request intervals
        # High CV = human (erratic), Low CV = bot (regular timing)
        if len(intervals) >= 2:
            mean_iv = statistics.mean(intervals)
            std_iv = statistics.stdev(intervals)
            interval_cv = std_iv / mean_iv if mean_iv > 0 else 0.0
        else:
            interval_cv = None  # not enough data

        return {
            'ip': session.get('ip', ''),
            'ts': session.get('start', datetime.utcnow()),
            'url_paths': url_paths,
            'ua': session.get('ua', ''),
            'num_requests': len(requests),
            'interval_cv': interval_cv,
            # fingerprint: JA3-like TLS hash (UA + cipher list order + Accept-Language).
            # Sessions from the same tool/library share the same fingerprint regardless
            # of source IP — a very strong coordinated-attack signal.
            'fingerprint': session.get('fingerprints', ''),
            'survey_country': session.get('survey_country', ''),
        }

    def _compute_uniformity(self, sessions: list) -> float:
        """Compute a 0.0–1.0 uniformity score over a cluster of session feature dicts.

        Higher score = more bot-like uniformity.
        Uses pre-computed session features (static_ratio, interval_cv) for accuracy.
        """
        if len(sessions) < CLUSTER_MIN_SESSIONS:
            return 0.0

        scores = []

        # 1. UA diversity: low unique-UA fraction → suspicious
        uas = [s['ua'] for s in sessions]
        ua_diversity = len(set(uas)) / len(uas)
        scores.append(max(0.0, 1.0 - ua_diversity * 3))  # 0 if > 33% unique UAs

        # 2. Fingerprint diversity: same TLS fingerprint across IPs → same tool → coordinated.
        # fingerprint is a JA3-like hash of (UA + cipher list order + Accept-Language).
        # Bots from the same library share one fingerprint even with different UAs.
        # Low diversity (< 50% unique) is a very strong coordinated-attack signal.
        fingerprints = [s['fingerprint'] for s in sessions if s.get('fingerprint')]
        if len(fingerprints) >= CLUSTER_MIN_SESSIONS:
            fp_diversity = len(set(fingerprints)) / len(fingerprints)
            scores.append(max(0.0, 1.0 - fp_diversity * 2))  # 0 if > 50% unique
        else:
            scores.append(0.0)

        # 3. URL path pattern uniformity: same top-level paths across IPs → suspicious
        # Use first path segment only to find clusters hitting the same section
        def top_path(paths):
            if not paths:
                return ''
            first = paths[0].split('/')
            return '/'.join(first[:3]) if len(first) >= 3 else paths[0]

        path_patterns = [top_path(s['url_paths']) for s in sessions if s['url_paths']]
        if len(path_patterns) >= CLUSTER_MIN_SESSIONS:
            top_ratio = Counter(path_patterns).most_common(1)[0][1] / len(path_patterns)
            scores.append(top_ratio)
        else:
            scores.append(0.0)

        # 4. Interval CV: low cluster-average CV → suspicious (humans have high variance)
        # interval_cv is None when session has < 2 intervals (skip those)
        cvs = [s['interval_cv'] for s in sessions if s.get('interval_cv') is not None]
        if len(cvs) >= 3:
            mean_cv = statistics.mean(cvs)
            # Humans: mean_cv > 0.8; bots: mean_cv < 0.3
            scores.append(max(0.0, 1.0 - mean_cv * 1.25))
        else:
            scores.append(0.0)

        # 5. Sequential numeric URL IDs across different IPs → suspicious
        all_paths = [p for s in sessions for p in s['url_paths']]
        numeric_paths = [p for p in all_paths if any(c.isdigit() for c in p)]
        if len(numeric_paths) > 5:
            unique_ratio = len(set(numeric_paths)) / len(numeric_paths)
            scores.append(min(1.0, (1.0 - unique_ratio) + 0.3))
        else:
            scores.append(0.0)

        # fingerprint (metric 2) gets primary weight — it's the strongest coordinated-attack
        # signal and subsumes UA identity. UA diversity (metric 1) is secondary.
        weights = [0.10, 0.50, 0.20, 0.15, 0.05]
        base_score = sum(s * w for s, w in zip(scores, weights))

        # LLM signal: blend in bot_ratio from session_llm_cache when available.
        # Only grey-zone sessions have verdicts, so coverage is partial — we blend
        # proportionally: LLM gets weight 0.30, existing metrics rescaled to 0.70.
        if self._session_llm_cache is not None:
            verdicts = [self._session_llm_cache.get(s['ip']) for s in sessions]
            verdicts_found = [v for v in verdicts if v is not None]
            if len(verdicts_found) >= 3:
                bot_ratio = sum(1 for v in verdicts_found if v.get('label') == 'bot') / len(verdicts_found)
                llm_weight = 0.30
                return min(1.0, base_score * (1.0 - llm_weight) + bot_ratio * llm_weight)

        return base_score

    def _check_clusters(self, cluster_buffer: dict, cluster_alerts: TTLCache, cluster_check_ts: dict, cluster_llm_ts: dict):
        """Per-host uniformity check, called once per main loop iteration.

        Phase 1: logging + cluster_alerts only, no LLM.
        """
        now = datetime.utcnow()
        for host, buf in list(cluster_buffer.items()):
            last_check = cluster_check_ts.get(host)
            if last_check and (now - last_check).total_seconds() < CLUSTER_CHECK_INTERVAL:
                continue

            cluster_check_ts[host] = now
            sessions = list(buf)

            if len(sessions) < CLUSTER_MIN_SESSIONS:
                continue

            unique_ips = len({s['ip'] for s in sessions if s.get('ip')})
            if unique_ips < CLUSTER_MIN_UNIQUE_IPS:
                self.logger.debug(
                    f"[CLUSTER] Skipping host={host} — only {unique_ips} unique IP(s), "
                    f"likely a single crawler"
                )
                continue

            # IP concentration check: detect UA-rotation scrapers that evade uniformity
            # scoring by using many different User-Agents and TLS fingerprints but still
            # originate from a single IP or /24 subnet (e.g. Tencent/Alibaba Cloud scrapers).
            if self._cluster_llm_queue is not None:
                ip_list = [s['ip'] for s in sessions if s.get('ip')]
                if ip_list:
                    ip_counts = Counter(ip_list)
                    top_ip, top_ip_count = ip_counts.most_common(1)[0]
                    ip_concentration = top_ip_count / len(ip_list)

                    subnet_counts = Counter('.'.join(ip.split('.')[:3]) for ip in ip_list)
                    top_subnet, top_subnet_count = subnet_counts.most_common(1)[0]
                    subnet_concentration = top_subnet_count / len(ip_list)

                    subnet16_counts = Counter('.'.join(ip.split('.')[:2]) for ip in ip_list)
                    top_subnet16, top_subnet16_count = subnet16_counts.most_common(1)[0]
                    subnet16_concentration = top_subnet16_count / len(ip_list)

                    if (ip_concentration >= CLUSTER_IP_CONCENTRATION
                            or subnet_concentration >= CLUSTER_SUBNET_CONCENTRATION
                            or subnet16_concentration >= CLUSTER_SUBNET16_CONCENTRATION):
                        self.logger.warning(
                            f"[CLUSTER_SCRAPER] High IP concentration host={host} "
                            f"top_ip={top_ip} ip_pct={ip_concentration:.0%} "
                            f"subnet={top_subnet}.x subnet_pct={subnet_concentration:.0%} "
                            f"subnet16={top_subnet16}.x.x subnet16_pct={subnet16_concentration:.0%} "
                            f"sessions={len(sessions)} ua_diversity="
                            f"{len({s['ua'] for s in sessions})/len(sessions):.2f}"
                        )
                        survey_country = next(
                            (s.get('survey_country', '') for s in sessions if s.get('survey_country')),
                            ''
                        )
                        try:
                            self._cluster_llm_queue.put_nowait({
                                'host': host,
                                'sessions': sessions,
                                'score': max(ip_concentration, subnet_concentration, subnet16_concentration),
                                'survey_country': survey_country,
                            })
                        except Exception:
                            pass

            score = self._compute_uniformity(sessions)

            # LLM verdict breakdown (for logging)
            llm_info = ''
            if self._session_llm_cache is not None:
                verdicts = [self._session_llm_cache.get(s['ip']) for s in sessions]
                verdicts_found = [v for v in verdicts if v is not None]
                if verdicts_found:
                    bot_count = sum(1 for v in verdicts_found if v.get('label') == 'bot')
                    llm_info = (
                        f" llm_coverage={len(verdicts_found)}/{len(sessions)}"
                        f" llm_bots={bot_count}/{len(verdicts_found)}"
                    )

            if score >= CLUSTER_ALERT_THRESHOLD:
                cluster_alerts[host] = score
                uas = [s['ua'] for s in sessions]
                ua_div = len(set(uas)) / len(uas)
                fps = [s['fingerprint'] for s in sessions if s.get('fingerprint')]
                fp_diversity = len(set(fps)) / len(fps) if fps else 1.0
                unique_fps = len(set(fps))
                cvs = [s['interval_cv'] for s in sessions if s.get('interval_cv') is not None]
                avg_cv = statistics.mean(cvs) if cvs else None
                self.logger.warning(
                    f"[CLUSTER] Suspicious uniformity host={host} score={score:.2f} "
                    f"sessions={len(sessions)} ua_diversity={ua_div:.2f} "
                    f"fp_diversity={fp_diversity:.2f} unique_fps={unique_fps} "
                    f"avg_interval_cv={f'{avg_cv:.2f}' if avg_cv is not None else 'n/a'}{llm_info}"
                )
                if score >= CLUSTER_LLM_THRESHOLD and self._cluster_llm_queue is not None:
                    last_llm = cluster_llm_ts.get(host)
                    llm_on_cooldown = last_llm and (now - last_llm).total_seconds() < CLUSTER_LLM_COOLDOWN
                    if llm_on_cooldown:
                        self.logger.debug(
                            f"[CLUSTER_LLM] Cooldown active host={host} score={score:.2f} "
                            f"next_in={(CLUSTER_LLM_COOLDOWN - (now - last_llm).total_seconds()):.0f}s"
                        )
                    else:
                        try:
                            survey_country = next(
                                (s.get('survey_country', '') for s in sessions if s.get('survey_country')),
                                ''
                            )
                            self._cluster_llm_queue.put_nowait({
                                'host': host,
                                'sessions': sessions,
                                'score': score,
                                'survey_country': survey_country,
                            })
                            cluster_llm_ts[host] = now
                            self.logger.info(
                                f"[CLUSTER_LLM] Enqueued analysis host={host} score={score:.2f}"
                            )
                        except Full:
                            self.logger.debug(f"[CLUSTER_LLM] Queue full, skipping host={host}")
            else:
                self.logger.debug(
                    f"[CLUSTER] host={host} score={score:.2f} sessions={len(sessions)}"
                    f" (below threshold){llm_info}"
                )

    # ------------------------------------------------------------------

    def get_shapley_report(self, shap_value, feature_names):
        """Legacy method for compatibility."""
        shapley_report = []
        min_shapley = 0
        shapley_feature = None
        for k, feature in enumerate(feature_names):
            value = shap_value.values[k]
            data_val = shap_value.data[k]
            if value < 0:
                if value < min_shapley:
                    min_shapley = value
                    shapley_feature = feature
                shapley_report.append(
                    {
                        "name": feature,
                        "values": {"shapley": round(value, 2), "feature": round(data_val, 2)},
                    }
                )
        shapley_report_sorted = sorted(
            shapley_report, key=lambda x: abs(x["values"]["shapley"]), reverse=True
        )
        return shapley_feature, shapley_report_sorted

    @staticmethod
    def _get_top_url(session):
        """Most frequent non-static URL in the session, query string stripped."""
        from collections import Counter
        urls = [
            r.get('url', '').split('?')[0]
            for r in session.get('requests', [])
            if r.get('url') and not r.get('static', False)
        ]
        if not urls:
            return ''
        return Counter(urls).most_common(1)[0][0][:200]

    def create_command(
            self,
            command_name,
            session,
            meta,
            prediction_if,
            score_if,
            shapley_if,
            shapley_feature_if,
            prediction_ae,
            score_ae,
            shapley_ae,
            shapley_feature_ae,
            difficulty,
            scraper_name,
            threshold_ae,
            rate_limit_hits=0,
            rate_limit_interval=0,
            rate_limit_expiration=0,
            baskerville_score=0,
            novel_attack=False,
            novel_attack_count=0,
    ):
        if novel_attack:
            meta = f"{meta} [novel_attack count={novel_attack_count}]"
        d = {
            "Name": command_name,
            "difficulty": difficulty,
            "Value": session["ip"],
            "country": session.get("country", ""),
            "continent": session.get("continent", ""),
            "datacenter_code": session.get("datacenter_code", ""),
            "session_id": session["session_id"],
            "host": session["host"],
            "source": meta,
            "shapley": shapley_if,
            "shapley_if": shapley_if,
            "shapley_ae": shapley_ae,
            "meta": meta,
            "prediction_if": int(prediction_if),
            "prediction_ae": int(prediction_ae),
            "shapley_feature": shapley_feature_if,
            "shapley_feature_if": shapley_feature_if,
            "shapley_feature_ae": shapley_feature_ae,
            "start": session["start"],
            "end": session["end"],
            "duration": session["duration"],
            "score": float(score_if),
            "score_if": float(score_if),
            "score_ae": float(score_ae),
            "bot_score": session.get("bot_score", 0.0),
            "bot_score_top_factor": session.get("bot_score_top_factor", ""),
            "num_requests": len(session.get("requests", [])),
            "user_agent": session.get("ua"),
            "human": session.get("human", ""),
            "datacenter_asn": session.get("datacenter_asn", False),
            "asn_name": session.get("asn_name", ""),
            "session": session,
            "scraper_name": scraper_name,
            "threshold_ae": float(threshold_ae),
            "rate_limit_hits": rate_limit_hits,
            "rate_limit_interval": rate_limit_interval,
            "rate_limit_expiration": rate_limit_expiration,
            "baskerville_score": int(baskerville_score),
            "cloudflare_score": session.get("cloudflare_score", 0),
            "survey_country": session.get("survey_country", ""),
            "ua": session.get("ua", ""),
            "top_url": self._get_top_url(session),
            "api_ratio": session.get("api_ratio", 0.0),
            "path_only_to_request_ratio": session.get("path_only_to_request_ratio", 1.0),
            "fingerprints": session.get("fingerprints", ""),
            "novel_attack": novel_attack,
            "novel_attack_count": novel_attack_count,
        }
        return d

    def _process_batch_single_thread(self, args_list):
        """Process batch in single thread - simple and reliable"""
        results_flat = []

        # Determine if we should skip AutoEncoder processing based on lag
        skip_ae = self.current_lag > self.lag_high_threshold
        if skip_ae:
            self.logger.info(f"High lag detected ({self.current_lag}), skipping AutoEncoder processing for performance")

        # Process each (host, human) group
        for (host, human), sessions in args_list:
            try:
                batch_results = self._process_sessions_batch(host, human, sessions, skip_ae)
                results_flat.extend(batch_results)
            except Exception:
                self.logger.exception(f"[{host}] Failed to process batch, human={human}")
                continue

        return results_flat

    def _process_sessions_batch(self, host: str, human: bool, sessions: List[Dict], skip_ae: bool):
        for s in sessions:
            if "host" not in s or not s["host"]:
                self.logger.info(f"sesison wihouth host {s['host']} not found in session {s['session_id']}")
                self.logger.info(s)
                s["host"] = host

        """Process a batch of sessions for a specific host/human combination"""
        model_if = self.models_if.get_model(host, ModelType.HUMAN if human else ModelType.BOT)
        model_if_opposite = self.models_if.get_model(host, ModelType.BOT if human else ModelType.HUMAN)

        # Adaptive Shapley processing: disable when heavily lagging to speed up processing
        use_shapley = self.use_shapley and len(sessions) < 50  # Skip Shapley for large batches

        scores_if = shap_values_if = None
        if model_if:
            scores_if, shap_values_if, vectors_if = model_if.transform(
                sessions, use_shapley=use_shapley
            )

        # Opposite model scores — no Shapley needed, just scores for High/High detection
        scores_if_opposite = None
        if model_if_opposite:
            scores_if_opposite, _, _ = model_if_opposite.transform(sessions, use_shapley=False)

        # Skip AutoEncoder processing when lagging heavily
        scores_ae = shap_values_ae = None
        threshold_ae = 0.0
        if not skip_ae:
            model_ae = self.models_ae.get_model(host, ModelType.HUMAN if human else ModelType.BOT)
            if model_ae:
                scores_ae, shap_values_ae, _ = model_ae.transform(
                    sessions, use_shapley=use_shapley
                )
                threshold_ae = float(model_ae.threshold)

        scores_classifier = shap_values_classifier = None

        if self.use_baskerville_score:
            model_classifier = self.models_classifier.get_model(
                'global', ModelType.GENERIC)
            if model_classifier:
                model_classifier.logger = self.logger
                self.logger.info(f"Running Baskerville classifer for {host}, human={human}")
                predictions_classifier, scores_classifier, shap_values_classifier, features_df = (
                    model_classifier.transform(
                        sessions, use_shapley=True
                    ))

                if self.verbose_classifier:
                    for i in range(len(scores_classifier)):
                        shapley_feature_classfier, shapley_classifier = _shapley_report_classifier(
                            shap_values_classifier[i] if shap_values_classifier is not None else None,
                            model_classifier.get_all_features()
                        )

                        # Log feature values for this session
                        self.logger.info(f"\n{'=' * 80}")
                        self.logger.info(
                            f"Session {i}: ip={sessions[i]['ip']}, session_id={sessions[i].get('session_id', 'N/A')}")
                        self.logger.info(f"  UA: {sessions[i].get('ua', 'N/A')[:100]}")
                        self.logger.info(f"  Ciphers: {sessions[i].get('ciphers', 'N/A')}")
                        self.logger.info(f"  Accept-Language: {sessions[i].get('accept_language', 'N/A')}")
                        self.logger.info(f"  num_languages: {sessions[i].get('num_languages', 'N/A')}")
                        self.logger.info(f"  cipher_type: {sessions[i].get('cipher_type', 'N/A')}")
                        self.logger.info(
                            f"Baskerville score: {scores_classifier[i]}, Baskerville_1: {sessions[i].get('baskerville_score_1', 'N/A')}, Cloudflare score: {sessions[i].get('cloudflare_score', 0)}, bot: {predictions_classifier[i]}")

                        # Log ALL features actually used by the model
                        if features_df is not None and i < len(features_df):
                            feature_row = features_df.iloc[i]
                            # Get actual feature names from the model (not hardcoded list!)
                            feature_names_from_model = model_classifier.get_all_features() if model_classifier else feature_row.index.tolist()

                            self.logger.info(f"\nALL FEATURES USED IN MODEL ({len(feature_names_from_model)} total):")

                            # Log all features that are actually in the model
                            for feat in feature_names_from_model:
                                if feat in feature_row.index:
                                    value = feature_row[feat]
                                    # Format based on type
                                    if isinstance(value, (int, float)):
                                        self.logger.info(f"  {feat:45s} {value:10.4f}")
                                    else:
                                        self.logger.info(f"  {feat:45s} {value}")
                                else:
                                    self.logger.info(f"  {feat:45s} <MISSING>")

                        # Log SHAP values
                        self.logger.info(f"\nTop negative SHAP feature: {shapley_feature_classfier}")

                        # Separate positive and negative SHAP values
                        positive_shap = []
                        negative_shap = []
                        feature_names = model_classifier.get_all_features()
                        for k, feature in enumerate(feature_names):
                            val = shap_values_classifier[i][k]
                            if val > 0:
                                positive_shap.append({'name': feature, 'shapley': val})
                            elif val < 0:
                                negative_shap.append({'name': feature, 'shapley': val})

                        positive_shap.sort(key=lambda x: abs(x['shapley']), reverse=True)
                        negative_shap.sort(key=lambda x: abs(x['shapley']), reverse=True)

                        # In XGBoost binary classification:
                        # - Negative SHAP → pulls to class 0 (HUMAN)
                        # - Positive SHAP → pulls to class 1 (BOT)
                        self.logger.info(
                            f"\nSHAP contributors to HUMAN (negative, sum={sum([x['shapley'] for x in negative_shap]):.2f}):")
                        for j, item in enumerate(negative_shap[:10]):  # Top 10
                            self.logger.info(f"  {j + 1}. {item['name']:35s} {item['shapley']:7.3f}")

                        self.logger.info(
                            f"\nSHAP contributors to BOT (positive, sum={sum([x['shapley'] for x in positive_shap]):.2f}):")
                        for j, item in enumerate(positive_shap[:10]):  # Top 10
                            self.logger.info(f"  {j + 1}. {item['name']:35s} +{item['shapley']:6.3f}")

                        self.logger.info(f"{'=' * 80}\n")

        results = []
        for i, session in enumerate(sessions):
            session_copy = dict(session)
            if "requests" in session_copy:
                session_copy["requests"] = session_copy["requests"][:self.max_requests_in_command]

            # Isolation Forest
            if scores_if is not None:
                score_if = float(scores_if[i])
                sensitivity_shift = self.settings.get_sensitivity(host) * self.sensitivity_factor
                score_if -= sensitivity_shift
                prediction_if = bool(score_if < 0)
                shapley_feature_if, shapley_if = _safe_shapley_report(
                    shap_values_if[i] if shap_values_if else None, model_if.get_all_features()
                )
            else:
                score_if = 0.0
                prediction_if = False
                shapley_feature_if, shapley_if = "", ""

            # High/High: anomalous vs own baseline AND anomalous vs opposite baseline
            score_if_opposite = float(scores_if_opposite[i]) if scores_if_opposite is not None else 0.0
            novel_attack = prediction_if and score_if_opposite < 0

            # Autoencoder
            if scores_ae is not None:
                score_ae = float(scores_ae[i])
                prediction_ae = bool(score_ae > threshold_ae)
                shapley_feature_ae, shapley_ae = _safe_shapley_report(
                    shap_values_ae[i] if shap_values_ae else None, model_ae.get_all_features()
                )
            else:
                score_ae = 0.0
                prediction_ae = False
                shapley_feature_ae, shapley_ae = "", ""

            scraper_name = session.get(
                "scraper_name",
                detect_scraper(session.get("ua")),
            )
            dnet = session.get("dnet", '-')

            api_ratio = 0.0
            path_only_to_request_ratio = 1.0
            if shap_values_if and model_if:
                sv = shap_values_if[i]
                feats = model_if.get_all_features()
                for k, fname in enumerate(feats):
                    if fname == "api_ratio":
                        api_ratio = round(sv.data[k], 2)
                    elif fname == "path_only_to_request_ratio":
                        path_only_to_request_ratio = round(sv.data[k], 3)

            entropy = 1.0
            if model_if:
                if 'entropy' in model_if.get_all_features():
                    entropy = float(vectors_if.iloc[i]['entropy'])
                if 'path_only_to_request_ratio' in model_if.get_all_features():
                    path_only_to_request_ratio = round(float(vectors_if.iloc[i]['path_only_to_request_ratio']), 3)

            if human:
                if scores_classifier is not None:
                    baskerville_score = int(scores_classifier[i])
                else:
                    baskerville_score = session.get('human_score', 99)
            else:
                baskerville_score = session.get('human_score', 1)
            results.append(
                {
                    "host": host,
                    "dnet": dnet,
                    "human": human,
                    "session": session_copy,
                    "scraper_name": scraper_name,
                    "score_if": score_if,
                    "prediction_if": prediction_if,
                    "shapley_if": shapley_if,
                    "shapley_feature_if": shapley_feature_if,
                    "score_ae": score_ae,
                    "prediction_ae": prediction_ae,
                    "shapley_ae": shapley_ae,
                    "shapley_feature_ae": shapley_feature_ae,
                    "threshold_ae": threshold_ae,
                    "api_ratio": api_ratio,
                    "path_only_to_request_ratio": path_only_to_request_ratio,
                    "entropy": entropy,
                    "baskerville_score": baskerville_score,
                    "novel_attack": novel_attack,
                    "score_if_opposite": score_if_opposite,
                }
            )
        return results

    def _process_results(
            self,
            results_flat,
            producer,
            producer_output,
            pending_challenge_ip,
            pending_interactive_ip,
            pending_block_ip,
            host_ip_sessions,
            ip_with_sessions,
            pending_session,
            novel_attack_counts,
    ):
        """Process results and apply decisions"""
        processed_count = 0

        for r in results_flat:
            self._apply_decision_and_send(
                producer=producer,
                producer_output=producer_output,
                r=r,
                pending_challenge_ip=pending_challenge_ip,
                pending_interactive_ip=pending_interactive_ip,
                pending_block_ip=pending_block_ip,
                host_ip_sessions=host_ip_sessions,
                ip_with_sessions=ip_with_sessions,
                pending_session=pending_session,
                novel_attack_counts=novel_attack_counts,
            )

            processed_count += 1

        # # Flush Kafka producer periodically for large batches
        # if processed_count % 100 == 0 and self.current_lag > self.lag_moderate_threshold:
        producer.flush()
        producer_output.flush()

        self.logger.debug(f"Processed {processed_count} results")

    def send(self,
             producer,
             producer_output,
             payload,
             key,
             dnet):
        key = bytearray(payload['host'], encoding="utf8")
        producer.send(topic=self.topic_commands,
                      value=json.dumps(payload).encode("utf-8"),
                      key=key)

        if producer_output is not None:
            # do not send heavy fields to the commands

            output_payload = {}

            for k in [
                "Name",
                "difficulty",
                "Value",
                "country",
                "continent",
                "datacenter_code",
                "session_id",
                "host",
                "source",
                "shapley",
                "shapley_if",
                "shapley_ae",
                "meta",
                "prediction_if",
                "prediction_ae",
                "shapley_feature",
                "shapley_feature_if",
                "shapley_feature_ae",
                "start",
                "end",
                "duration",
                "score",
                "score_if",
                "score_ae",
                "bot_score",
                "bot_score_top_factor",
                "num_requests",
                "user_agent",
                "human",
                "datacenter_asn",
                "asn_name",
                "scraper_name",
                "threshold_ae",
                "rate_limit_hits",
                "rate_limit_interval",
                "rate_limit_expiration",
                "survey_country"
            ]:
                output_payload[k] = payload[k]

            output_payload['print_log'] = self.print_log_in_command

            payload_encoded = json.dumps(output_payload).encode("utf-8")
            partition = self.dnet_partition_map.get(dnet, -1)
            if partition < 0:
                self.logger.warning(f"Dnet  {dnet} is not found in "
                                    f"the dnet map {self.dnet_partition_map}.")
                producer_output.send(topic=self.topic_commands_output,
                                     value=payload_encoded,
                                     key=key)
            else:
                producer_output.send(topic=self.topic_commands_output,
                                     value=payload_encoded,
                                     partition=partition)

    def _apply_decision_and_send(
            self,
            producer: KafkaProducer,
            producer_output: KafkaProducer,
            r: dict,
            pending_challenge_ip: TTLCache,
            pending_interactive_ip: TTLCache,
            pending_block_ip: TTLCache,
            host_ip_sessions: dict,
            ip_with_sessions: TTLCache,
            pending_session: TTLCache,
            novel_attack_counts: TTLCache,
    ):
        host = r["host"]
        dnet = r["dnet"]
        human = r["human"]
        session = r["session"]
        scraper_name = r["scraper_name"]
        score_if = r["score_if"]
        prediction_if = r["prediction_if"]
        shapley_if = r["shapley_if"]
        shapley_feature_if = r["shapley_feature_if"]
        score_ae = r["score_ae"]
        prediction_ae = r["prediction_ae"]
        shapley_ae = r["shapley_ae"]
        shapley_feature_ae = r["shapley_feature_ae"]
        threshold_ae = r["threshold_ae"]
        api_ratio = r["api_ratio"]
        path_only_to_request_ratio = r.get("path_only_to_request_ratio", 1.0)
        entropy = r["entropy"]
        baskerville_score = r["baskerville_score"]
        novel_attack = r.get("novel_attack", False)
        score_if_opposite = r.get("score_if_opposite", 0.0)
        session_id = session["session_id"]
        session["api_ratio"] = api_ratio
        session["path_only_to_request_ratio"] = path_only_to_request_ratio

        ip = session["ip"]

        novel_attack_count = 0
        if novel_attack:
            novel_attack_count = novel_attack_counts.get(ip, 0) + 1
            novel_attack_counts[ip] = novel_attack_count
            self.logger.warning(
                f"[NOVEL_ATTACK] ip={ip} host={host} human={human} "
                f"score_if={score_if:.3f} score_if_opposite={score_if_opposite:.3f} "
                f"novel_attack_count_1h={novel_attack_count} ua={session.get('ua', '')[:80]}"
            )

        primary_session = session.get("primary_session", False)
        verified_bot = session.get("verified_bot", False)
        verified_ai_bot = session.get("verified_ai_bot", False)
        if verified_bot or session.get("asset_only", False) or verified_ai_bot:
            if verified_bot:
                self.logger.info(
                    f"Skipping verified_bot ip={ip} host={host} "
                    f"bot={session.get('verified_bot_name', '')} session_id={session_id}"
                )
            elif verified_ai_bot:
                self.logger.info(
                    f"Skipping verified_ai_bot ip={ip} host={host} "
                    f"bot={session.get('verified_ai_bot_name', '')} session_id={session_id}"
                )
            return

        # Queue for session-level LLM analysis (observation only, no action taken here).
        # Two paths into grey zone:
        #   1) baskerville_score in configured range (when classifier model is loaded)
        #   2) ML anomaly detected on human session (fallback when classifier is absent)
        _llm_score_in_range = self._session_llm_score_min <= baskerville_score <= self._session_llm_score_max
        _llm_ml_anomaly = prediction_if or prediction_ae
        if (
            self._session_llm_enabled
            and self._session_llm_queue is not None
            and human
            and not session.get('bad_bot', False)
            and len(session.get('requests', [])) >= self._session_llm_min_requests
            and (_llm_score_in_range or _llm_ml_anomaly)
        ):
            try:
                self._session_llm_queue.put_nowait({
                    'session': session,
                    'host': host,
                    'baskerville_score': baskerville_score,
                    'dnet': dnet,
                })
            except Full:
                pass  # queue full — skip, main loop must not block

        if session.get("ai_spoofer", False):
            if ip not in pending_block_ip:
                pending_block_ip[ip] = True
                self.logger.warning(
                    f"[AI_SPOOFER] block_ip ip={ip} host={host} "
                    f"asn={session.get('asn_name', '')} ua={session.get('ua', '')[:120]}"
                )
                payload = self.create_command(
                    command_name="block_ip",
                    session=session,
                    meta="ai_spoofer",
                    prediction_if=prediction_if,
                    score_if=score_if,
                    shapley_if=shapley_if,
                    shapley_feature_if=shapley_feature_if,
                    prediction_ae=prediction_ae,
                    score_ae=score_ae,
                    shapley_ae=shapley_ae,
                    shapley_feature_ae=shapley_feature_ae,
                    difficulty=0,
                    scraper_name=scraper_name,
                    threshold_ae=threshold_ae,
                    rate_limit_hits=self.rate_limit_hits,
                    rate_limit_interval=self.rate_limit_interval,
                    rate_limit_expiration=self.rate_limit_expiration,
                    baskerville_score=baskerville_score,
                )
                self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        # AI crawler blocking (PetalBot, AhrefsBot, CCBot, etc.)
        # These are commercial/AI crawlers that provide zero benefit to our clients.
        # They openly declare themselves via UA, so blocking is surgical and accurate.
        # verified_ai_bot (DNS-verified) is already handled above (skip without action).
        if session.get("ai_bot_ua", False):
            if ip not in pending_block_ip:
                pending_block_ip[ip] = True
                self.logger.warning(
                    f"[AI_BOT] block_ip ip={ip} host={host} "
                    f"ua={session.get('ua', '')[:120]}"
                )
                payload = self.create_command(
                    command_name="block_ip",
                    session=session,
                    meta="ai_bot_ua",
                    prediction_if=prediction_if,
                    score_if=score_if,
                    shapley_if=shapley_if,
                    shapley_feature_if=shapley_feature_if,
                    prediction_ae=prediction_ae,
                    score_ae=score_ae,
                    shapley_ae=shapley_ae,
                    shapley_feature_ae=shapley_feature_ae,
                    difficulty=0,
                    scraper_name=scraper_name,
                    threshold_ae=threshold_ae,
                    baskerville_score=baskerville_score,
                )
                self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        # Commercial crawler blocking (AhrefsBot, SemrushBot, MJ12bot, etc.)
        # These are paid SEO/analytics products that crawl for their customers' benefit.
        # No benefit to our clients — block directly. Controlled by BLOCK_COMMERCIAL_CRAWLERS.
        if self.block_commercial_crawlers and session.get("commercial_crawler", False):
            if ip not in pending_block_ip:
                pending_block_ip[ip] = True
                self.logger.warning(
                    f"[COMMERCIAL_CRAWLER] block_ip ip={ip} host={host} "
                    f"ua={session.get('ua', '')[:120]}"
                )
                payload = self.create_command(
                    command_name="block_ip",
                    session=session,
                    meta="commercial_crawler",
                    prediction_if=prediction_if,
                    score_if=score_if,
                    shapley_if=shapley_if,
                    shapley_feature_if=shapley_feature_if,
                    prediction_ae=prediction_ae,
                    score_ae=score_ae,
                    shapley_ae=shapley_ae,
                    shapley_feature_ae=shapley_feature_ae,
                    difficulty=0,
                    scraper_name=scraper_name,
                    threshold_ae=threshold_ae,
                    baskerville_score=baskerville_score,
                )
                self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        # WordPress credential brute force detection.
        # High 4xx ratio + enough requests = credential stuffing bot (curl/python, not a browser).
        # challenge_ip won't help — these clients can't solve JS challenges. Block directly.
        _4xx_ratio = session.get("response4xx_to_request_ratio", 0.0)
        _num_requests = session.get("num_requests", 0)
        if _4xx_ratio > 0.8 and _num_requests >= 15:
            if ip not in pending_block_ip:
                pending_block_ip[ip] = True
                self.logger.warning(
                    f"[BRUTE_FORCE] block_ip ip={ip} host={host} "
                    f"4xx_ratio={_4xx_ratio:.2f} num_requests={_num_requests} "
                    f"ua={session.get('ua', '')[:80]}"
                )
                payload = self.create_command(
                    command_name="block_ip",
                    session=session,
                    meta="brute_force_4xx",
                    prediction_if=prediction_if,
                    score_if=score_if,
                    shapley_if=shapley_if,
                    shapley_feature_if=shapley_feature_if,
                    prediction_ae=prediction_ae,
                    score_ae=score_ae,
                    shapley_ae=shapley_ae,
                    shapley_feature_ae=shapley_feature_ae,
                    difficulty=0,
                    scraper_name=scraper_name,
                    threshold_ae=threshold_ae,
                    baskerville_score=baskerville_score,
                )
                self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        # WordPress login brute force detection.
        # wp-login.php returns 200 even on failed auth, so 4xx_ratio stays low.
        # Detect by path concentration: all requests hammering the same login endpoint.
        # challenge_ip won't help — block directly.
        _top_url = self._get_top_url(session)
        _top_ratio = session.get('top_page_to_request_ratio', 0.0)
        if ('wp-login' in (_top_url or '') and _top_ratio > 0.8 and _num_requests >= 5):
            if ip not in pending_block_ip:
                pending_block_ip[ip] = True
                self.logger.warning(
                    f"[WP_BRUTE_FORCE] block_ip ip={ip} host={host} "
                    f"top_url={_top_url} top_ratio={_top_ratio:.2f} num_requests={_num_requests}"
                )
                payload = self.create_command(
                    command_name="block_ip",
                    session=session,
                    meta="wp_brute_force",
                    prediction_if=prediction_if,
                    score_if=score_if,
                    shapley_if=shapley_if,
                    shapley_feature_if=shapley_feature_if,
                    prediction_ae=prediction_ae,
                    score_ae=score_ae,
                    shapley_ae=shapley_ae,
                    shapley_feature_ae=shapley_feature_ae,
                    difficulty=0,
                    scraper_name=scraper_name,
                    threshold_ae=threshold_ae,
                    baskerville_score=baskerville_score,
                )
                self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        if not session.get("primary_session", False):
            ip_with_sessions[session["ip"]] = True

        # Survey country protection — computed early, used in both responder and attack response blocks
        survey_country = session.get("survey_country", "")
        session_country = session.get("country", "")
        protected_country = bool(
            survey_country and session_country and session_country == survey_country
        )

        # Responder actions: LLM-issued targeted blocks by country, ASN, or UA
        responder = self._first_responder_actions.get(host)
        if responder:
            ra_action = responder['action']
            ra_target = responder['target']
            ra_hit = False
            ra_meta = ''
            if ra_action == 'block_country' and session_country and session_country in ra_target:
                ra_hit = True
                ra_meta = 'first_responder [block_country] [block_ip]'
            elif ra_action == 'block_asn':
                asn_name = session.get('asn_name', '')
                if asn_name and asn_name in ra_target:
                    ra_hit = True
                    ra_meta = 'first_responder [block_asn] [block_ip]'
            elif ra_action == 'block_ua':
                session_ua = session.get('ua', '')
                if session_ua and session_ua in ra_target:
                    ra_hit = True
                    ra_meta = 'first_responder [block_ua] [block_ip]'

            if ra_hit and (ra_action == 'block_ua' or not protected_country):
                if ip not in pending_block_ip:
                    pending_block_ip[ip] = True
                    self.logger.warning(
                        f"{ra_meta} block_ip ip={ip} host={host} "
                        f"country={session_country} asn={session.get('asn_name', '')} "
                        f"session_id={session_id}"
                    )
                    payload = self.create_command(
                        command_name="block_ip",
                        session=session,
                        meta=ra_meta,
                        prediction_if=prediction_if,
                        score_if=score_if,
                        shapley_if=shapley_if,
                        shapley_feature_if=shapley_feature_if,
                        prediction_ae=prediction_ae,
                        score_ae=score_ae,
                        shapley_ae=shapley_ae,
                        shapley_feature_ae=shapley_feature_ae,
                        difficulty=0,
                        scraper_name=scraper_name,
                        threshold_ae=threshold_ae,
                        baskerville_score=1,
                        novel_attack=novel_attack,
                        novel_attack_count=novel_attack_count,
                    )
                    self.send(producer, producer_output, payload, key=host, dnet=dnet)
                return

        spike_ratio = self._attack_response_hosts.get(host, 0.0)
        attack_response = spike_ratio >= self._attack_min_spike_ratio          # 4.0+: classifier threshold 50
        attack_response_aggressive = spike_ratio >= self._attack_aggressive_spike_ratio   # 6.0+: bad_bot → block
        attack_response_extreme = spike_ratio >= self._attack_extreme_spike_ratio         # 15.0+: datacenter_asn → block

        # DDoS attack mode EXTREME: block datacenter ASN immediately
        if attack_response_extreme and session.get("datacenter_asn", False):
            if ip not in pending_block_ip:
                pending_block_ip[ip] = True
                extreme_command = "block_ip"
                self.logger.warning(
                    f"datacenter_asn {extreme_command} [attack_response] ip={ip} host={host} "
                    f"session_id={session_id} human={human}"
                )
                payload = self.create_command(
                    command_name=extreme_command,
                    session=session,
                    meta=f"datacenter_asn [attack_response] [{extreme_command}]",
                    prediction_if=prediction_if,
                    score_if=score_if,
                    shapley_if=shapley_if,
                    shapley_feature_if=shapley_feature_if,
                    prediction_ae=prediction_ae,
                    score_ae=score_ae,
                    shapley_ae=shapley_ae,
                    shapley_feature_ae=shapley_feature_ae,
                    difficulty=0,
                    scraper_name=scraper_name,
                    threshold_ae=threshold_ae,
                    baskerville_score=1,
                    novel_attack=novel_attack,
                    novel_attack_count=novel_attack_count,
                )
                self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        # High bot score from banjax fingerprinting → block.
        # This check must come BEFORE the classifier challenge path because the classifier
        # path returns early and would shadow banjax's bot_score signal.
        bot_score = session.get("bot_score", 0.0)
        bot_score_top_factor = session.get("bot_score_top_factor", "")
        if (
                session.get("passed_challenge")
                and bot_score > self.bot_score_threshold
                and bot_score_top_factor != "no_payload"
        ):
            if ip in pending_block_ip:
                return
            pending_block_ip[ip] = True
            baskerville_score = 10
            self.logger.info(
                f"High bot score - block_ip for ip={ip}, "
                f"human={human}, session_id={session_id}, host={host}, "
                f"top_factor={bot_score_top_factor} threshold={self.bot_score_threshold}  "
                f"baskerville_score={baskerville_score}  "
                f"cloudflare_score={session.get('cloudflare_score', 0)}."
            )
            payload = self.create_command(
                command_name="block_ip",
                session=session,
                meta="high_bot_score [block_ip]",
                prediction_if=prediction_if,
                score_if=score_if,
                shapley_if=shapley_if,
                shapley_feature_if=shapley_feature_if,
                prediction_ae=prediction_ae,
                score_ae=score_ae,
                shapley_ae=shapley_ae,
                shapley_feature_ae=shapley_feature_ae,
                difficulty=0,
                scraper_name=scraper_name,
                threshold_ae=threshold_ae,
                rate_limit_hits=self.rate_limit_hits,
                rate_limit_interval=self.rate_limit_interval,
                rate_limit_expiration=self.rate_limit_expiration,
                baskerville_score=baskerville_score,
                novel_attack=novel_attack,
                novel_attack_count=novel_attack_count,
            )
            self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        # High baskerville_score — threshold raised to 50 under DDoS attack
        classifier_threshold = 50 if attack_response else 30
        if human and 0 < baskerville_score < classifier_threshold:
            if ip not in pending_session:
                pending_session[ip] = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
            if session_id in pending_session[ip]:
                return
            pending_session[ip][session_id] = True
            command = "challenge_session"

            self.logger.info(
                f"Classifier {command} for ip={ip}, "
                f"human={human}, command={command}, session_id={session_id}, host={host}, "
                f"baskerville_score={baskerville_score}  "
                f"cloudflare_score={session.get('cloudflare_score', 0)}."
            )
            payload = self.create_command(
                command_name=command,
                session=session,
                meta="classifier [attack_response] [challenge_session]" if attack_response else "classifier [challenge_session]",
                prediction_if=prediction_if,
                score_if=score_if,
                shapley_if=shapley_if,
                shapley_feature_if=shapley_feature_if,
                prediction_ae=prediction_ae,
                score_ae=score_ae,
                shapley_ae=shapley_ae,
                shapley_feature_ae=shapley_feature_ae,
                difficulty=0,
                scraper_name=scraper_name,
                threshold_ae=threshold_ae,
                rate_limit_hits=self.rate_limit_hits,
                rate_limit_interval=self.rate_limit_interval,
                rate_limit_expiration=self.rate_limit_expiration,
                baskerville_score=baskerville_score,
                novel_attack=novel_attack,
                novel_attack_count=novel_attack_count,
            )
            self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        if self.bad_bot_challenge and session.get("bad_bot") and ip not in ip_with_sessions.keys():
            if ip in pending_challenge_ip:
                return
            pending_challenge_ip[ip] = True

            command = "challenge_ip"

            num_non_static = len(session.get("requests", []))
            if entropy == 0 and num_non_static > 1:
                command = "block_ip"

            # Very short/empty UA can't be a real browser — won't solve JS challenge
            if len(session.get("ua", "") or "") < 5:
                command = "block_ip"

            if host == 'antijob.net' or attack_response_aggressive:
                command = "block_ip"
            # else:
            #     command = "rate_limit" if self.use_rate_limit else "challenge_ip"

            baskerville_score = 1
            self.logger.info(f"{command} ip (bad_bot),"
                             f"Baskerville score {baskerville_score}, "
                             f"Cloudflare score {session.get('cloudflare_score', 0)}, "
                             f"ip = {ip} host={host}  "
                             f"ua={session.get('ua')} end={session.get('end')}")

            payload = self.create_command(
                command_name=command,
                session=session,
                meta=f"Bad bot rule{' [attack_response]' if attack_response else ''} [{command}]",
                prediction_if=prediction_if,
                score_if=score_if,
                shapley_if=shapley_if,
                shapley_feature_if=shapley_feature_if,
                prediction_ae=prediction_ae,
                score_ae=score_ae,
                shapley_ae=shapley_ae,
                shapley_feature_ae=shapley_feature_ae,
                difficulty=0,
                scraper_name=scraper_name,
                threshold_ae=threshold_ae,
                rate_limit_hits=self.rate_limit_hits,
                rate_limit_interval=self.rate_limit_interval,
                rate_limit_expiration=self.rate_limit_expiration,
                baskerville_score=baskerville_score,
                novel_attack=novel_attack,
                novel_attack_count=novel_attack_count,
            )
            self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        # weak_cipher / scraper meta rule
        meta = None
        if session.get("weak_cipher", False):
            meta = "weak_cipher"
        elif scraper_name is not None and len(scraper_name) > 0 and self.challenge_scrapers:
            meta = "scraper"
        if meta:
            if ip in pending_challenge_ip:
                return
            pending_challenge_ip[ip] = True
            command = "challenge_ip"  # rate_limit not supported by banjax
            meta = f"{meta} [{command}]"

            baskerville_score = 20
            self.logger.info(
                f"meta {meta} - {command} for ip={ip}, "
                f"human={human}, command={command}, session_id={session_id}, host={host}, "
                f"ua={session.get('ua')}  "
                f"baskerville_score={baskerville_score}. "
                f"cloudflare_score={session.get('cloudflare_score', 0)} end={session.get('end')}."
            )
            payload = self.create_command(
                command_name=command,
                session=session,
                meta=meta,
                prediction_if=prediction_if,
                score_if=score_if,
                shapley_if=shapley_if,
                shapley_feature_if=shapley_feature_if,
                prediction_ae=prediction_ae,
                score_ae=score_ae,
                shapley_ae=shapley_ae,
                shapley_feature_ae=shapley_feature_ae,
                difficulty=0,
                scraper_name=scraper_name,
                threshold_ae=threshold_ae,
                baskerville_score=baskerville_score,
                novel_attack=novel_attack,
                novel_attack_count=novel_attack_count,
            )
            self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        if not primary_session:
            if host not in host_ip_sessions:
                host_ip_sessions[host] = TTLCache(
                    maxsize=self.maxsize_ip_sessions, ttl=120 * 60
                )
            if ip not in host_ip_sessions[host]:
                host_ip_sessions[host][ip] = TTLCache(
                    maxsize=self.maxsize_ip_sessions,
                    ttl=self.ip_sessions_ttl_in_minutes * 60,
                )
            host_ip_sessions[host][ip][session_id] = True
            if len(host_ip_sessions[host][ip]) >= self.max_sessions_for_ip:
                if ip in pending_challenge_ip:
                    return
                pending_challenge_ip[ip] = True

                baskerville_score = 25
                self.logger.info(
                    f"Too many sessions ({len(host_ip_sessions[host][ip])}) challenge_ip for ip={ip}, "
                    f"human={human} session_id={session_id}, host={host}, "
                    f"ua={session.get('ua')}  "
                    f"baskerville_score={baskerville_score}. "
                    f"cloudflare_score={session.get('cloudflare_score', 0)} end={session.get('end')}."
                )

                payload = self.create_command(
                    command_name="challenge_ip",
                    session=session,
                    meta="Too many sessions. [challenge_ip]",
                    prediction_if=prediction_if,
                    score_if=score_if,
                    shapley_if=shapley_if,
                    shapley_feature_if=shapley_feature_if,
                    prediction_ae=prediction_ae,
                    score_ae=score_ae,
                    shapley_ae=shapley_ae,
                    shapley_feature_ae=shapley_feature_ae,
                    difficulty=0,
                    scraper_name=scraper_name,
                    threshold_ae=threshold_ae,
                    baskerville_score=baskerville_score,
                    novel_attack=novel_attack,
                    novel_attack_count=novel_attack_count,
                )
                self.send(producer, producer_output, payload, key=host, dnet=dnet)
                return

        if prediction_if or prediction_ae:
            if api_ratio == 1.0:
                self.logger.info(f"Skipping challenge for ip={ip}, host={host} since api_ratio is 1.0")
                return
            num_non_static = len(session.get("requests", []))
            duration = session.get("duration", 0)
            if primary_session:
                if not human:
                    command = "block_ip" if attack_response_aggressive else "challenge_ip"
                else:
                    if ip in pending_challenge_ip:
                        return
                    pending_challenge_ip[ip] = True
                    command = "challenge_ip"  # rate_limit not supported by banjax
            else:
                if ip not in pending_session:
                    pending_session[ip] = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
                if session_id in pending_session[ip]:
                    return
                pending_session[ip][session_id] = True
                if human:
                    command = "challenge_session"
                else:
                    command = "block_ip" if attack_response_aggressive else "challenge_ip"  # rate_limit not supported

            baskerville_score = baskerville_score if not human else 25
            self.logger.info(
                f"Anomaly {command} for ip={ip}, human={human}, command={command}, "
                f"session_id={session_id}, host={host}, "
                f"score_if={score_if}, score_ae={score_ae},  "
                f"baskerville_score={baskerville_score}, baskerville_score_1={session.get('baskerville_score_1', 'N/A')}, "
                f"cloudflare_score={session.get('cloudflare_score', 0)} end={session.get('end')}."
            )
            payload = self.create_command(
                command_name=command,
                session=session,
                meta=f"anomaly{' [attack_response]' if attack_response else ''} [{command}]",
                prediction_if=prediction_if,
                score_if=score_if,
                shapley_if=shapley_if,
                shapley_feature_if=shapley_feature_if,
                prediction_ae=prediction_ae,
                score_ae=score_ae,
                shapley_ae=shapley_ae,
                shapley_feature_ae=shapley_feature_ae,
                difficulty=0,
                scraper_name=scraper_name,
                threshold_ae=threshold_ae,
                rate_limit_hits=self.rate_limit_hits,
                rate_limit_interval=self.rate_limit_interval,
                rate_limit_expiration=self.rate_limit_expiration,
                baskerville_score=baskerville_score,
                novel_attack=novel_attack,
                novel_attack_count=novel_attack_count,
            )
            self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        # # send not positive prediction only to main storage producer(not producer_ouput)
        # self.logger.info(
        #     f"No command for ip={ip}, human={human}, "
        #     f"session_id={session_id}, host={host}, "
        #     f"baskerville_score={baskerville_score}, baskerville_score_1={session.get('baskerville_score_1', 'N/A')}, "
        #     f"cloudflare_score={session.get('cloudflare_score', 0)},  end={session.get('end')}."
        # )
        # payload = self.create_command(
        #     command_name='no command',
        #     session=session,
        #     meta="",
        #     prediction_if=prediction_if,
        #     score_if=score_if,
        #     shapley_if=shapley_if,
        #     shapley_feature_if=shapley_feature_if,
        #     prediction_ae=prediction_ae,
        #     score_ae=score_ae,
        #     shapley_ae=shapley_ae,
        #     shapley_feature_ae=shapley_feature_ae,
        #     difficulty=0,
        #     scraper_name=scraper_name,
        #     threshold_ae=threshold_ae,
        #     rate_limit_hits=self.rate_limit_hits,
        #     rate_limit_interval=self.rate_limit_interval,
        #     rate_limit_expiration=self.rate_limit_expiration,
        #     baskerville_score=baskerville_score,
        # )
        # self.send(producer, None, payload, key=host, dnet=dnet)

    def process_immature_session(self, session):
        self.logger.info(f"Immature session is_human={is_human(session)}, "
                         f"len={len(session['requests'])}, "
                         f"score1 = {session.get('baskerville_score_1', 'N/A')}, "
                         f"score2 = {session.get('baskerville_score_2', 'N/A')}, "
                         f"ip={session['ip']}, "
                         f"session_id={session['session_id']} ")

    def create_kafka_connections(self, max_retries=10, initial_delay=5):
        consumer = None
        producer = None
        producer_output = None

        for attempt in range(max_retries):
            try:
                self.logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})...")
                consumer = KafkaConsumer(
                    **self.kafka_connection,
                    group_id=self.group_id,
                    max_poll_records=self.batch_size,
                    fetch_max_bytes=52428800 * 5,
                    max_partition_fetch_bytes=1048576 * 10,
                    max_poll_interval_ms=self.max_poll_interval_ms,
                    fetch_max_wait_ms=self.fetch_max_wait_ms,
                    fetch_min_bytes=self.fetch_min_bytes,
                    session_timeout_ms=45000,  # 1 minutes
                    enable_auto_commit=True,
                    auto_offset_reset='latest',
                )
                listener = RebalanceLogger(self.logger, name=self.hostname)
                consumer.subscribe([self.topic_sessions], listener=listener)

                # wait (up to ~30s) for a real assignment
                start = time_module.time()
                while not consumer.assignment():
                    consumer.poll(timeout_ms=1000)
                    if time_module.time() - start > 30:
                        break
                self.logger.info(f"Assigned: {consumer.assignment()}")

                producer = KafkaProducer(
                    **self.kafka_connection
                )

                producer_output = KafkaProducer(
                    **self.kafka_connection_output
                )
                return consumer, producer, producer_output

            except NoBrokersAvailable as e:
                delay = initial_delay * (2 ** attempt)
                self.logger.warning(
                    f"No Kafka brokers available (attempt {attempt + 1}/{max_retries}). Retrying in {delay}s... Error: {e}")
                time_module.sleep(delay)
                if consumer:
                    try:
                        consumer.close()
                    except:
                        pass
                if producer:
                    try:
                        producer.close()
                    except:
                        pass
            except Exception as e:
                delay = initial_delay * (2 ** attempt)
                self.logger.error(f"Unexpected Kafka error (attempt {attempt + 1}/{max_retries}): {e}")
                time_module.sleep(delay)
                if consumer:
                    try:
                        consumer.close()
                    except:
                        pass
                if producer:
                    try:
                        producer.close()
                    except:
                        pass
                if producer_output:
                    try:
                        producer_output.close()
                    except:
                        pass
        raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")

    def run(self):
        pending_challenge_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        pending_interactive_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        pending_block_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        host_ip_sessions: Dict[str, TTLCache] = dict()
        pending_session = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        novel_attack_counts = TTLCache(maxsize=50000, ttl=60 * 60)  # ip → count, 1h window

        if self._session_llm_enabled:
            self._session_llm_cache = TTLCache(maxsize=5000, ttl=3600)  # ip → verdict, 1h cooldown
            self._session_llm_queue = Queue(maxsize=self._session_llm_queue_size)
            threading.Thread(target=self._llm_session_worker, daemon=True, name='session-llm').start()
            self.logger.info(
                f"[SESSION_LLM] Enabled: provider={self._session_llm_provider} model={self._llm_model} "
                f"score_range=[{self._session_llm_score_min},{self._session_llm_score_max}] "
                f"min_requests={self._session_llm_min_requests}"
            )

        # Cluster analysis — uniformity scoring + LLM incident creation
        cluster_buffer: Dict[str, deque] = defaultdict(lambda: deque(maxlen=CLUSTER_BUFFER_MAXLEN))
        cluster_alerts: TTLCache = TTLCache(maxsize=500, ttl=600)  # TTL 10 min
        cluster_check_ts: Dict[str, datetime] = {}
        cluster_llm_ts: Dict[str, datetime] = {}
        if self._session_llm_enabled and self._openai_api_key:
            self._cluster_llm_queue = Queue(maxsize=20)
            threading.Thread(target=self._cluster_llm_worker, daemon=True, name='cluster-llm').start()
            self.logger.info(
                f"[CLUSTER] Uniformity scoring + LLM enabled: check_interval={CLUSTER_CHECK_INTERVAL}s "
                f"min_sessions={CLUSTER_MIN_SESSIONS} alert_threshold={CLUSTER_ALERT_THRESHOLD} "
                f"llm_threshold={CLUSTER_LLM_THRESHOLD}"
            )
        else:
            self.logger.info(
                f"[CLUSTER] Uniformity scoring enabled (LLM disabled): "
                f"check_interval={CLUSTER_CHECK_INTERVAL}s "
                f"min_sessions={CLUSTER_MIN_SESSIONS} alert_threshold={CLUSTER_ALERT_THRESHOLD}"
            )

        offences = TTLCache(maxsize=10000, ttl=60 * 60)
        ip_with_sessions = TTLCache(maxsize=100000, ttl=60 * 60)

        self.logger.info("Starting predictor...")
        try:

            consumer, producer, producer_output = self.create_kafka_connections()

            self.logger.info(
                f"Starting predicting on topic {self.topic_sessions}"
            )
            self.logger.info(f"debug_ip={self.debug_ip}")
            ts_lag_report = datetime.now()
            ts_assign_report = datetime.utcnow()

            while True:
                if (datetime.utcnow() - ts_assign_report).total_seconds() > 30:
                    log_partition_assignment(consumer, self.logger)
                    ts_assign_report = datetime.utcnow()

                self._refresh_attack_response()
                self._check_clusters(cluster_buffer, cluster_alerts, cluster_check_ts, cluster_llm_ts)

                raw_messages = consumer.poll(timeout_ms=self.kafka_poll_timeout_ms, max_records=self.batch_size)
                for topic_partition, messages in raw_messages.items():
                    batch: Dict[Tuple[str, bool], List[dict]] = defaultdict(list)
                    self.logger.info(f"Batch size {len(messages)}")
                    predicting_total = 0
                    ip_whitelisted = 0

                    for message in messages:
                        if (datetime.now() - ts_lag_report).total_seconds() > 5:
                            try:
                                end = consumer.end_offsets([topic_partition]).get(
                                    topic_partition)  # end offset (next to be written)
                            except Exception:
                                end = None

                            last_off = messages[-1].offset if messages else None
                            if end is not None and last_off is not None:
                                # messages remaining after the last one we just processed
                                lag = max(0, end - (last_off + 1))
                            else:
                                lag = 0
                            self.current_lag = lag
                            self.logger.info(f"Lag = {lag} (adaptive processing: {self.adaptive_processing})")
                            ts_lag_report = datetime.now()

                        if not message.value:
                            continue

                        session = json.loads(message.value.decode("utf-8"))
                        human = session.get("human", False)
                        ip = session["ip"]
                        host = message.key.decode("utf-8")

                        if self.settings.is_ip_whitelisted(host, session["ip"]):
                            ip_whitelisted += 1
                            continue

                        if host == 'report.if.ua' and session.get('ai_bot_ua', False):
                            continue

                        if session.get("deflect_password", False):
                            continue

                        if session.get('immature_session', False):
                            self.process_immature_session(session)
                            continue

                        if not session.get("primary_session", False):
                            ip_with_sessions[session["ip"]] = True

                        session["host"] = host
                        if (not session.get('verified_bot', False)
                                and not session.get('verified_ai_bot', False)
                                and not session.get('ai_spoofer', False)):
                            cluster_buffer[host].append(self._extract_cluster_features(session))
                        batch[(host, human)].append(session)
                        predicting_total += 1

                    args_list = list(batch.items())
                    if not args_list:
                        self.logger.info(
                            f"batch={len(messages)}, predicting_total = {predicting_total}, whitelisted = {ip_whitelisted}"
                        )
                        continue

                    # Single-threaded batch processing
                    results_flat = self._process_batch_single_thread(args_list)

                    # Apply decisions and send commands
                    self._process_results(
                        results_flat=results_flat,
                        producer=producer,
                        producer_output=producer_output,
                        pending_challenge_ip=pending_challenge_ip,
                        pending_interactive_ip=pending_interactive_ip,
                        pending_block_ip=pending_block_ip,
                        host_ip_sessions=host_ip_sessions,
                        ip_with_sessions=ip_with_sessions,
                        pending_session=pending_session,
                        novel_attack_counts=novel_attack_counts,
                    )

                    self.logger.info(
                        f"batch={len(messages)}, predicting_total = {predicting_total}, whitelisted = {ip_whitelisted}"
                    )
                    producer.flush()
                    producer_output.flush()
        except Exception as ex:
            self.logger.exception(f'Exception in consumer loop:{ex}')

        self.logger.info("Predictor finished")
