# -*- coding: utf-8 -*-
"""
Baskervillehall Predictor (multiprocessing version)

- Воркеры (ProcessPoolExecutor) только считают: IF/AE, shapley, api_ratio и т.п.
- Решения (challenge/block/skip), все TTLCache и Kafka — строго в главном процессе.
- Устраняет "Can't pickle local object ..." (top-level worker function + initializer).
"""

import json
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple, Dict, Any

from cachetools import TTLCache
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from baskervillehall.baskervillehall_isolation_forest import (
    BaskervillehallIsolationForest,
    ModelType,
)
from baskervillehall.model_storage import ModelStorage
from baskervillehall.settings_deflect_api import SettingsDeflectAPI
from baskervillehall.settings_postgres import SettingsPostgres
from baskervillehall.whitelist_ip import WhitelistIP

from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError

import multiprocessing as mp


# ===== module-level globals for ProcessPool workers =====
_g_models_if = None
_g_models_ae = None
_g_settings = None
_g_use_shapley = None
_g_sensitivity_factor = None
_g_max_requests_in_command = None


def _init_worker(
    s3_connection: dict,
    s3_path_if: str,
    s3_path_ae: str,
    model_reload_in_minutes: int,
    settings_kind: str,
    settings_kwargs: dict,
    use_shapley: bool,
    sensitivity_factor: float,
    max_requests_in_command: int,
):
    """
    Runs once per worker process: инициализация моделей и настроек.
    """
    global _g_models_if, _g_models_ae, _g_settings
    global _g_use_shapley, _g_sensitivity_factor, _g_max_requests_in_command

    _g_models_if = ModelStorage(
        s3_connection,
        s3_path_if,
        reload_in_minutes=model_reload_in_minutes,
    )
    _g_models_ae = ModelStorage(
        s3_connection,
        s3_path_ae,
        reload_in_minutes=model_reload_in_minutes,
    )

    if settings_kind == "postgres":
        _g_settings = SettingsPostgres(**settings_kwargs)
    else:
        _g_settings = SettingsDeflectAPI(**settings_kwargs)

    _g_use_shapley = use_shapley
    _g_sensitivity_factor = sensitivity_factor
    _g_max_requests_in_command = max_requests_in_command


def _safe_shapley_report(sv, feature_names):
    """Возвращает ('', []) если sv отсутствует или структура неожиданная."""
    if sv is None:
        return '', []
    try:
        return _shapley_report(sv, feature_names)
    except Exception:
        return '', []

def _shapley_report(shap_value, feature_names):
    """
    Возвращает:
      - имя top фичи по модулю отрицательного shapley
      - отсортированный список отрицательных вкладов
    """
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


def process_item_worker(args: Tuple[Tuple[str, bool], List[Dict[str, Any]], bool]):
    """
    Pure worker: получает ((host, human), sessions, skip_ae) и возвращает list из per-session dict.
    НЕТ Kafka, НЕТ TTLCache, только расчёты.
    """
    (host, human), sessions, skip_ae = args

    model_if = _g_models_if.get_model(host, ModelType.HUMAN if human else ModelType.BOT)
    
    # Adaptive Shapley processing: disable when heavily lagging to speed up processing
    use_shapley = _g_use_shapley and len(sessions) < 50  # Skip Shapley for large batches

    scores_if = shap_values_if = None
    if model_if:
        scores_if, shap_values_if = model_if.transform(
            sessions, use_shapley=use_shapley
        )

    # Skip AutoEncoder processing when lagging heavily
    scores_ae = shap_values_ae = None
    threshold_ae = 0.0
    if not skip_ae:
        model_ae = _g_models_ae.get_model(host, ModelType.HUMAN if human else ModelType.BOT)
        if model_ae:
            scores_ae, shap_values_ae = model_ae.transform(
                sessions, use_shapley=use_shapley
            )
            threshold_ae = float(model_ae.threshold)

    out = []
    for i, session in enumerate(sessions):
        # копия + трим запросов
        session_copy = dict(session)
        if "requests" in session_copy:
            session_copy["requests"] = session_copy["requests"][:_g_max_requests_in_command]

        # Isolation Forest
        if scores_if is not None:
            score_if = float(scores_if[i])
            sensitivity_shift = _g_settings.get_sensitivity(host) * _g_sensitivity_factor
            score_if -= sensitivity_shift
            prediction_if = bool(score_if < 0)
            shapley_feature_if, shapley_if = _safe_shapley_report(
                shap_values_if[i] if shap_values_if else None, model_if.get_all_features()
            )
        else:
            score_if = 0.0
            prediction_if = False
            shapley_feature_if, shapley_if = "", ""

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
            BaskervillehallIsolationForest.detect_scraper(session.get("ua")),
        )

        # api_ratio (если есть такая фича)
        api_ratio = 0.0
        if shap_values_if and model_if:
            sv = shap_values_if[i]
            feats = model_if.get_all_features()
            for k, fname in enumerate(feats):
                if fname == "api_ratio":
                    api_ratio = round(sv.data[k], 2)
                    break

        out.append(
            {
                "host": host,
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
            }
        )
    return out


def is_static_session(session: Dict[str, Any]) -> bool:
    for r in session.get("requests", []):
        if not r.get("static", False):
            return False
    return True


class BaskervillehallPredictor(object):
    def __init__(
        self,
        topic_sessions="BASKERVILLEHALL_SESSIONS",
        partition=0,
        num_partitions=3,
        topic_commands="banjax_command_topic",
        topic_reports="banjax_report_topic",
        kafka_connection=None,
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
        whitelist_ip=None,
        deflect_config_url=None,
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
    ):
        super().__init__()

        if s3_connection is None:
            s3_connection = {}
        if postgres_connection is None:
            postgres_connection = {}
        if kafka_connection is None:
            kafka_connection = {"bootstrap_servers": "localhost:9092"}

        self.topic_sessions = topic_sessions
        self.partition = partition
        self.num_partitions = num_partitions
        self.topic_commands = topic_commands
        self.kafka_connection = kafka_connection
        self.s3_connection = s3_connection
        self.postgres_connection = postgres_connection
        self.s3_path = s3_path
        self.min_session_duration = min_session_duration
        self.min_number_of_requests = min_number_of_requests
        self.white_list_refresh_in_minutes = white_list_refresh_in_minutes
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.whitelist_ip = whitelist_ip
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
        self.white_list_refresh_period = white_list_refresh_period
        self.bad_bot_challenge = bad_bot_challenge
        self.use_shapley = use_shapley
        self.max_sessions_for_ip = max_sessions_for_ip
        self.maxsize_ip_sessions = maz_size_ip_sessions
        self.ip_sessions_ttl_in_minutes = ip_sessions_ttl_in_minutes
        self.max_requests_in_command = max_requests_in_command
        self.bot_score_threshold = bot_score_threshold
        self.challenge_scrapers = challenge_scrapers

        if deflect_config_url is None or len(deflect_config_url) == 0:
            self.settings = SettingsPostgres(
                refresh_period_in_seconds=postgres_refresh_period_in_seconds,
                postgres_connection=postgres_connection,
            )
        else:
            self.settings = SettingsDeflectAPI(
                url=self.deflect_config_url,
                logger=self.logger,
                refresh_period_in_seconds=60 * self.white_list_refresh_period,
            )
        self.sensitivity_factor = sensitivity_factor

    def get_shapley_report(self, shap_value, feature_names):
        """
        Оставлено для совместимости / потенциального использования в main-процессе.
        """
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
    ):
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
            "shapley_feature": shapley_feature_if if len(shapley_feature_if) > 0 else shapley_feature_ae,
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
            "datacenter_asn": session.get("datacenter_asn", ""),
            "session": session,
            "scraper_name": scraper_name,
            "threshold_ae": float(threshold_ae),
        }
        return json.dumps(d).encode("utf-8")

    def _process_batch_parallel(self, executor, args_list):
        """Process batch using true parallelism with futures and memory management"""
        # Adaptive chunk sizing based on lag
        if self.current_lag > self.lag_high_threshold:
            # High lag - use smaller chunks for faster processing
            chunk_size = min(len(args_list), max(1, len(args_list) // (2 * os.cpu_count())))
        else:
            # Normal processing - use standard chunk size
            chunk_size = min(len(args_list), max(1, len(args_list) // os.cpu_count()))
        
        # Create optimal chunks
        chunks = [args_list[i:i + chunk_size] for i in range(0, len(args_list), chunk_size)]
        
        # Determine if we should skip AutoEncoder processing based on lag
        skip_ae = self.current_lag > self.lag_high_threshold
        if skip_ae:
            self.logger.info(f"High lag detected ({self.current_lag}), skipping AutoEncoder processing for performance")

        # Submit all tasks in parallel
        futures = []
        for chunk in chunks:
            if self.current_lag > self.lag_high_threshold and len(chunk) > 1:
                # Split large chunks further when lagging heavily
                for item in chunk:
                    # Add skip_ae parameter to worker args
                    worker_args = (item[0], item[1], skip_ae)
                    futures.append(executor.submit(process_item_worker, worker_args))
            else:
                # Process multiple items together
                for item in chunk:
                    # Add skip_ae parameter to worker args
                    worker_args = (item[0], item[1], skip_ae)
                    futures.append(executor.submit(process_item_worker, worker_args))
        
        # Collect results with timeout handling
        results_flat = []
        timeout = 40 if self.current_lag < self.lag_moderate_threshold else 10  # Faster timeout when lagging
        
        try:
            for future in as_completed(futures, timeout=timeout):
                try:
                    chunk_result = future.result(timeout=5)
                    results_flat.extend(chunk_result)
                    # Clear result to free memory
                    del chunk_result
                except Exception as e:
                    self.logger.warning(f"Worker task failed: {e}")
        except TimeoutError:
            # Handle timeout gracefully - collect completed futures and log incomplete ones
            completed_count = 0
            incomplete_count = 0
            for future in futures:
                if future.done():
                    try:
                        chunk_result = future.result()
                        results_flat.extend(chunk_result)
                        del chunk_result
                        completed_count += 1
                    except Exception as e:
                        self.logger.warning(f"Completed future had error: {e}")
                        completed_count += 1
                else:
                    future.cancel()  # Cancel incomplete futures
                    incomplete_count += 1
            
            self.logger.warning(f"Processing timeout after {timeout}s: {completed_count} completed, {incomplete_count} cancelled")
        
        # Clear futures to free memory
        del futures
        return results_flat
    
    def _process_results_with_memory_management(
        self, 
        results_flat, 
        producer, 
        pending_challenge_ip, 
        pending_interactive_ip, 
        pending_block_ip, 
        host_ip_sessions, 
        ip_with_sessions, 
        pending_session
    ):
        """Process results with aggressive memory management for large batches"""
        processed_count = 0
        
        for r in results_flat:
            self._apply_decision_and_send(
                producer=producer,
                r=r,
                pending_challenge_ip=pending_challenge_ip,
                pending_interactive_ip=pending_interactive_ip,
                pending_block_ip=pending_block_ip,
                host_ip_sessions=host_ip_sessions,
                ip_with_sessions=ip_with_sessions,
                pending_session=pending_session,
            )
            
            # Clear result immediately to free memory
            del r
            processed_count += 1
            
            # Flush Kafka producer periodically for large batches
            if processed_count % 100 == 0 and self.current_lag > self.lag_moderate_threshold:
                producer.flush()
        
        self.logger.debug(f"Processed {processed_count} results")

    def _apply_decision_and_send(
        self,
        producer: KafkaProducer,
        r: dict,
        pending_challenge_ip: TTLCache,
        pending_interactive_ip: TTLCache,  # оставлен для совместимости
        pending_block_ip: TTLCache,
        host_ip_sessions: dict,
        ip_with_sessions: TTLCache,
        pending_session: TTLCache,
    ):
        """
        Применяет правила (как в твоей исходной логике) и, если нужно, отправляет команду в Kafka.
        Все кэши и producer — только здесь, в главном процессе.
        """
        host = r["host"]
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

        ip = session["ip"]
        primary_session = session.get("primary_session", False)
        verified_bot = session.get("verified_bot", False)
        if verified_bot or session.get("asset_only", False):
            return

        # если не primary_session — помечаем, что у ip есть сессии
        if not session.get("primary_session", False):
            ip_with_sessions[session["ip"]] = True

        # High bot score -> block
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
            self.logger.info(
                f"High bot score = {bot_score}, human={human}, top_factor = {bot_score_top_factor} for ip {ip}, host {host}. Blocking."
            )
            payload = self.create_command(
                command_name="block_ip_testing",
                session=session,
                meta="high_bot_score",
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
            )
            producer.send(self.topic_commands, payload, key=bytearray(host, encoding="utf8"))
            return

        # bad_bot rule vs known sessions
        if self.bad_bot_challenge and session.get("bad_bot") and ip not in ip_with_sessions.keys():
            if ip in pending_challenge_ip:
                return
            pending_challenge_ip[ip] = True
            self.logger.info(
                f'Challenging for ip={ip} (bad_bot_challenge), ua={session.get("ua")}, host={host}, end={session.get("end")}.'
            )
            payload = self.create_command(
                command_name="challenge_ip",
                session=session,
                meta="Bad bot rule",
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
            )
            producer.send(self.topic_commands, payload, key=bytearray(host, encoding="utf8"))
            return

        # weak_cipher / scraper meta rule
        meta = None
        if session.get("weak_cipher", False):
            meta = "weak_cipher"
        elif len(scraper_name) > 0 and self.challenge_scrapers:
            meta = "scraper"
        if meta:
            if ip in pending_challenge_ip:
                return
            pending_challenge_ip[ip] = True
            self.logger.info(
                f'Challenging for ip={ip} meta={meta}, ua={session.get("ua")}, host={host}, end={session.get("end")}.'
            )
            payload = self.create_command(
                command_name="challenge_ip",
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
            )
            producer.send(self.topic_commands, payload, key=bytearray(host, encoding="utf8"))
            return

        # учёт сессий на IP для non-primary
        session_id = session["session_id"]
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
                self.logger.info(
                    f"Too many sessions ({len(host_ip_sessions[host][ip])}) for ip {ip}, host {host} -> challenge"
                )
                payload = self.create_command(
                    command_name="challenge_ip",
                    session=session,
                    meta="Too many sessions.",
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
                )
                producer.send(self.topic_commands, payload, key=bytearray(host, encoding="utf8"))
                return

        # основное правило по моделям
        if prediction_if or prediction_ae:
            if api_ratio == 1.0:
                self.logger.info(f"Skipping challenge for ip={ip}, host={host} since api_ratio is 1.0")
                return

            if primary_session:
                if ip in pending_challenge_ip:
                    return
                pending_challenge_ip[ip] = True
                command = "challenge_ip"
            else:
                if ip not in pending_session:
                    pending_session[ip] = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
                if session_id in pending_session[ip]:
                    return
                pending_session[ip][session_id] = True
                command = "challenge_session"

            self.logger.info(
                f"Challenging for ip={ip}, command={command}, session_id={session_id}, host={host}, score_if={score_if}, score_ae={score_ae}."
            )
            payload = self.create_command(
                command_name=command,
                session=session,
                meta="",
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
            )
            producer.send(self.topic_commands, payload, key=bytearray(host, encoding="utf8"))
            return
        # иначе — ничего не делаем

    def run(self):
        pending_challenge_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        pending_interactive_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        pending_block_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        host_ip_sessions: Dict[str, TTLCache] = dict()
        pending_session = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)

        offences = TTLCache(maxsize=10000, ttl=60 * 60)  # оставлено на будущее
        ip_with_sessions = TTLCache(maxsize=100000, ttl=60 * 60)

        settings_kind = "postgres" if isinstance(self.settings, SettingsPostgres) else "deflect_api"
        if settings_kind == "postgres":
            settings_kwargs = {
                "refresh_period_in_seconds": self.settings.refresh_period_in_seconds,
                "postgres_connection": self.postgres_connection,
            }
        else:
            settings_kwargs = {
                "url": self.deflect_config_url,
                # "logger": self.logger,
                "refresh_period_in_seconds": 60 * self.white_list_refresh_period,
            }

        ctx = mp.get_context("spawn")
        self.logger.info("Creating ProcessPool (spawn)...")
        executor = ProcessPoolExecutor(
            max_workers=os.cpu_count(),
            mp_context=ctx,
            initializer=_init_worker,
            initargs=(
                self.s3_connection,
                self.s3_path,
                f"{self.s3_path}_autoencoder3",
                self.model_reload_in_minutes,
                settings_kind,
                settings_kwargs,
                self.use_shapley,
                self.sensitivity_factor,
                self.max_requests_in_command,
            ))

        consumer = KafkaConsumer(
            **self.kafka_connection,
            max_poll_records=self.batch_size,
            fetch_max_bytes=52428800 * 5,
            max_partition_fetch_bytes=1048576 * 10,
            max_poll_interval_ms=self.max_poll_interval_ms,
            fetch_max_wait_ms=self.fetch_max_wait_ms,
            fetch_min_bytes=self.fetch_min_bytes,
            session_timeout_ms=300000,  # 5 minutes
        )

        producer = KafkaProducer(
            **self.kafka_connection
        )

        self.logger.info(
            f"Starting predicting on topic {self.topic_sessions}, partition {self.partition}"
        )
        self.logger.info(f"debug_ip={self.debug_ip}")
        whitelist_ip = WhitelistIP(
            self.whitelist_ip,
            logger=self.logger,
            refresh_period_in_seconds=60 * self.white_list_refresh_in_minutes,
        )

        consumer.assign([TopicPartition(self.topic_sessions, self.partition)])
        consumer.seek_to_end()
        ts_lag_report = datetime.now()

        while True:
            raw_messages = consumer.poll(timeout_ms=self.kafka_poll_timeout_ms, max_records=self.batch_size)
            for topic_partition, messages in raw_messages.items():
                batch: Dict[Tuple[str, bool], List[dict]] = defaultdict(list)
                self.logger.info(f"Batch size {len(messages)}")
                predicting_total = 0
                ip_whitelisted = 0

                for message in messages:
                    if (datetime.now() - ts_lag_report).total_seconds() > 5:
                        highwater = consumer.highwater(topic_partition)
                        lag = (highwater - 1) - message.offset
                        self.current_lag = lag
                        self.logger.info(f"Lag = {lag} (adaptive processing: {self.adaptive_processing})")
                        ts_lag_report = datetime.now()

                    if not message.value:
                        continue

                    session = json.loads(message.value.decode("utf-8"))
                    human = session.get("human", False)
                    ip = session["ip"]
                    host = message.key.decode("utf-8")

                    if whitelist_ip.is_in_whitelist(host, session["ip"]):
                        ip_whitelisted += 1
                        continue

                    if session.get("deflect_password", False):
                        continue

                    if not session.get("primary_session", False):
                        ip_with_sessions[session["ip"]] = True

                    # обязательные поля в командах
                    session["host"] = host
                    batch[(host, human)].append(session)
                    predicting_total += 1

                # === multiprocessing: расчёты в воркерах ===
                args_list = list(batch.items())
                if not args_list:
                    # нечего обрабатывать
                    self.logger.info(
                        f"batch={len(messages)}, predicting_total = {predicting_total}, whitelisted = {ip_whitelisted}"
                    )
                    continue

                # Optimized batch processing with true parallelism
                results_flat = self._process_batch_parallel(executor, args_list)

                # === решения + отправка (главный процесс) ===
                self._process_results_with_memory_management(
                    results_flat=results_flat,
                    producer=producer,
                    pending_challenge_ip=pending_challenge_ip,
                    pending_interactive_ip=pending_interactive_ip,
                    pending_block_ip=pending_block_ip,
                    host_ip_sessions=host_ip_sessions,
                    ip_with_sessions=ip_with_sessions,
                    pending_session=pending_session,
                )
                
                # Clear memory
                del results_flat
                del batch

                self.logger.info(
                    f"batch={len(messages)}, predicting_total = {predicting_total}, whitelisted = {ip_whitelisted}"
                )
                producer.flush()

        executor.shutdown(wait=True, cancel_futures=True)
