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
import time as time_module
from collections import defaultdict
from datetime import datetime
from typing import List, Tuple, Dict, Any

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
from baskervillehall.whitelist_ip import WhitelistIP




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
        whitelist_ip=None,
        deflect_config_url=None,
        deflect_config_auth=None,
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
        rate_limit_hits=20,
        rate_limit_interval=60,
        rate_limit_expiration=300,
        use_rate_limit=True,
        dnet_partition_map = None,
        print_log_in_command = True,
        use_baskerville_score=True,
        verbose_classifier=False,
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
                logger=self.logger,
                refresh_period_in_seconds=60 * self.white_list_refresh_period,
            )
        self.sensitivity_factor = sensitivity_factor

        # Initialize models directly in main thread
        self.models_if = ModelStorage(
            s3_connection,
            s3_path,
            reload_in_minutes=model_reload_in_minutes,
        )
        self.models_ae = ModelStorage(
            s3_connection,
            f"{s3_path}_autoencoder3",
            reload_in_minutes=model_reload_in_minutes,
        )
        self.models_classifier = ModelStorage(
            s3_connection,
            f"{s3_path}_classifier",
            reload_in_minutes=model_reload_in_minutes,
        )

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
        baskerville_score=0
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
            "datacenter_asn": session.get("datacenter_asn", ""),
            "session": session,
            "scraper_name": scraper_name,
            "threshold_ae": float(threshold_ae),
            "rate_limit_hits": rate_limit_hits,
            "rate_limit_interval": rate_limit_interval,
            "rate_limit_expiration": rate_limit_expiration,
            "baskerville_score": int(baskerville_score),
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
        
        # Adaptive Shapley processing: disable when heavily lagging to speed up processing
        use_shapley = self.use_shapley and len(sessions) < 50  # Skip Shapley for large batches

        scores_if = shap_values_if = None
        if model_if:
            scores_if, shap_values_if, vectors_if = model_if.transform(
                sessions, use_shapley=use_shapley
            )

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

        if human and self.use_baskerville_score:
            model_classifier = self.models_classifier.get_model(
                'global', ModelType.GENERIC)
            if model_classifier:
                self.logger.info(f"Running Baskerville classifer for {host}, human={human}")
                predictions_classifier, scores_classifier, shap_values_classifier, _ = (
                    model_classifier.transform(
                        sessions, use_shapley=True
                    ))

                if self.verbose_classifier:
                    for i in range(len(scores_classifier)):
                        shapley_feature_classfier, shapley_classifier = _safe_shapley_report(
                            shap_values_classifier[i] if shap_values_classifier is not None else None,
                            model_classifier.get_all_features()
                        )
                        self.logger.info(shap_values_classifier[i] )
                        self.logger.info(f"Baskerville score {scores_classifier[i]}, "
                                         f"Cloudflare score {sessions[i].get('cloudflare_score', 0)}, "
                                         f"ip = {sessions[i]['ip']} "
                                         f"bot = {predictions_classifier[i]}")
                        self.logger.info(f"Shapley feature {shapley_feature_classfier}, ")
                        self.logger.info(shapley_classifier)

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
            if shap_values_if and model_if:
                sv = shap_values_if[i]
                feats = model_if.get_all_features()
                for k, fname in enumerate(feats):
                    if fname == "api_ratio":
                        api_ratio = round(sv.data[k], 2)
                        break

            entropy = 1.0
            if model_if:
                if 'entropy' in model_if.get_all_features():
                    entropy = float(vectors_if.iloc[i]['entropy'])

            if human:
                if scores_classifier is not None:
                    baskerville_score = scores_classifier[i]
                else:
                    baskerville_score = 99
            else:
                baskerville_score = 1
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
                    "entropy": entropy,
                    "baskerville_score": baskerville_score,
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
        pending_session
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
            payload.pop('session')
            payload.pop('baskerville_score')
            payload['print_log'] = self.print_log_in_command

            payload_encoded = json.dumps(payload).encode("utf-8")
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
        entropy = r["entropy"]
        baskerville_score = r["baskerville_score"]
        session_id = session["session_id"]

        ip = session["ip"]
        primary_session = session.get("primary_session", False)
        verified_bot = session.get("verified_bot", False)
        if verified_bot or session.get("asset_only", False):
            return

        if not session.get("primary_session", False):
            ip_with_sessions[session["ip"]] = True

        # High baskerville_score
        if human and baskerville_score < 30 and baskerville_score > 0:
            if ip not in pending_session:
                pending_session[ip] = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
            if session_id in pending_session[ip]:
                return
            pending_session[ip][session_id] = True
            command = "challenge_session"

            self.logger.info(
                f"Classifier {command} for ip={ip}, human={human}, command={command}, session_id={session_id}, host={host}, "
                f"baskerville_score={baskerville_score}."
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
                rate_limit_hits=self.rate_limit_hits,
                rate_limit_interval=self.rate_limit_interval,
                rate_limit_expiration=self.rate_limit_expiration,
                baskerville_score=baskerville_score,
            )
            self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return


        # High bot score -> block
        bot_score = session.get("bot_score", 0.0)
        bot_score_top_factor = session.get("bot_score_top_factor", "")
        if (
            human
            and session.get("passed_challenge")
            and bot_score > self.bot_score_threshold
            and bot_score_top_factor != "no_payload"
        ):
            if ip in pending_block_ip:
                return
            pending_block_ip[ip] = True
            command = "block_ip"
            self.logger.info(
                f"{command} High bot score = {bot_score}, human={human}, top_factor = {bot_score_top_factor} for ip {ip}, host {host}."
            )
            payload = self.create_command(
                command_name=command,
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
                rate_limit_hits=self.rate_limit_hits,
                rate_limit_interval=self.rate_limit_interval,
                rate_limit_expiration=self.rate_limit_expiration
            )
            self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

        if self.bad_bot_challenge and session.get("bad_bot") and ip not in ip_with_sessions.keys():
            if ip in pending_challenge_ip:
                return
            pending_challenge_ip[ip] = True
            if entropy == 0:
                command = "block_ip"
            else:
                command = "rate_limit" if self.use_rate_limit else "challenge_ip"

            self.logger.info(
                f'{command} for ip={ip} (bad_bot), ua={session.get("ua")}, host={host}, end={session.get("end")}.'
            )
            payload = self.create_command(
                command_name=command,
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
                rate_limit_hits=self.rate_limit_hits,
                rate_limit_interval=self.rate_limit_interval,
                rate_limit_expiration=self.rate_limit_expiration
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
            command_name = "rate_limit" if self.use_rate_limit else "challenge_ip"
            self.logger.info(
                f'{command_name} for ip={ip} human={human}, meta={meta}, ua={session.get("ua")}, host={host}, end={session.get("end")}.'
            )
            payload = self.create_command(
                command_name=command_name,
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
                self.logger.info(
                    f"Too many sessions ({len(host_ip_sessions[host][ip])}) for ip {ip}, human={human}, host {host} -> challenge"
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
                self.send(producer, producer_output, payload, key=host, dnet=dnet)
                return

        if prediction_if or prediction_ae:
            if api_ratio == 1.0:
                self.logger.info(f"Skipping challenge for ip={ip}, host={host} since api_ratio is 1.0")
                return

            if primary_session:
                if ip in pending_challenge_ip:
                    return
                pending_challenge_ip[ip] = True
                command = "rate_limit" if self.use_rate_limit else "challenge_ip"
            else:
                if ip not in pending_session:
                    pending_session[ip] = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
                if session_id in pending_session[ip]:
                    return
                pending_session[ip][session_id] = True
                if human:
                    command = "challenge_session"
                else:
                    command = "rate_limit" if self.use_rate_limit else "challenge_ip"

            self.logger.info(
                f"{command} for ip={ip}, human={human}, command={command}, session_id={session_id}, host={host}, score_if={score_if}, score_ae={score_ae}."
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
                rate_limit_hits=self.rate_limit_hits,
                rate_limit_interval=self.rate_limit_interval,
                rate_limit_expiration=self.rate_limit_expiration
            )
            self.send(producer, producer_output, payload, key=host, dnet=dnet)
            return

    def run(self):
        pending_challenge_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        pending_interactive_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        pending_block_ip = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)
        host_ip_sessions: Dict[str, TTLCache] = dict()
        pending_session = TTLCache(maxsize=self.maxsize_pending, ttl=self.pending_ttl)

        offences = TTLCache(maxsize=10000, ttl=60 * 60)
        ip_with_sessions = TTLCache(maxsize=100000, ttl=60 * 60)

        self.logger.info("Starting predictor...")

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
        consumer.subscribe([self.topic_sessions])

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

        self.logger.info(
            f"Starting predicting on topic {self.topic_sessions}"
        )
        self.logger.info(f"debug_ip={self.debug_ip}")
        whitelist_ip = WhitelistIP(
            self.whitelist_ip,
            logger=self.logger,
            refresh_period_in_seconds=60 * self.white_list_refresh_in_minutes,
        )

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

                    if whitelist_ip.is_in_whitelist(host, session["ip"]):
                        ip_whitelisted += 1
                        continue

                    if session.get("deflect_password", False):
                        continue

                    if not session.get("primary_session", False):
                        ip_with_sessions[session["ip"]] = True

                    session["host"] = host
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
                )

                self.logger.info(
                    f"batch={len(messages)}, predicting_total = {predicting_total}, whitelisted = {ip_whitelisted}"
                )
                producer.flush()
                producer_output.flush()

        self.logger.info("Predictor finished")
