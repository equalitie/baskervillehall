"""
Classifier trainer for baskerville_score_4 (global XGBoost model).

Unlike baskervillehall_trainer.py which trains per-domain Isolation Forest models,
this trainer:
  - Collects sessions from ALL domains combined (global model)
  - Uses rule-based labeling (bot/human) instead of unsupervised anomaly detection
  - Trains a single XGBoost BaskervilleClassifier
  - Saves to {s3_path}_classifier/global/generic/
  - Retrains on a fixed TTL (default 4 hours)
"""

import gc
import json
import logging
import math
import time
import time as time_module
from collections import Counter

import numpy as np
from kafka import KafkaConsumer

from baskervillehall.baskerville_classifier import BaskervilleClassifier
from baskervillehall.baskervillehall_isolation_forest import ModelType
from baskervillehall.model_io import ModelIO

# Session fields needed for feature extraction and labeling
_SESSION_KEYS = {
    'requests', 'duration', 'start', 'timezone',
    'ciphers', 'num_languages', 'ua_score', 'fingerprints_score',
    'primary_session', 'bad_bot', 'human', 'passed_challenge',
    'valid_browser_ciphers', 'weak_cipher', 'headless_ua', 'bot_ua',
    'ai_bot_ua', 'verified_bot', 'short_ua', 'asset_only',
    'country', 'cipher', 'cipher_type', 'datacenter_asn',
    'request_rate',
}

_REQUEST_KEYS = {'ts', 'code', 'url', 'payload', 'edge', 'ua', 'query', 'type', 'method', 'static'}


def _strip_session(session):
    """Strip session to only the fields needed for training, reducing memory."""
    stripped = {k: session[k] for k in _SESSION_KEYS if k in session}
    if 'requests' in stripped:
        stripped['requests'] = [
            {k: r[k] for k in _REQUEST_KEYS if k in r}
            for r in stripped['requests']
        ]
    return stripped


def _entropy(requests):
    """Shannon entropy of full URL distribution (url + query string)."""
    if not requests:
        return 0.0
    urls = []
    for r in requests:
        url = r.get('url', '')
        query = r.get('query', '')
        full = url + ('?' + query if query else '')
        urls.append(full)
    counter = Counter(urls)
    total = len(urls)
    ent = 0.0
    for count in counter.values():
        p = count / total
        if p > 0:
            ent -= p * math.log2(p)
    return ent


def _label_session(session):
    """
    Labeling for XGBoost classifier training.
    Uses only high-confidence labels based on direct flags, NOT behavioral thresholds.

    Design rationale: entropy, request_rate and other behavioral features are used by the
    model as input features. If we also use them to generate labels (e.g. "entropy < 0.5 → bot"),
    the model simply memorizes the threshold rule rather than learning generalizable patterns.
    Instead, we label only the most obvious cases and let the model discover the behavioral
    correlates on its own.

    Returns 1 (bot), 0 (human), or None (uncertain — exclude from training).
    """
    # --- Obvious bots: identified by UA detection flags ---
    # These sessions are labeled bot because the UA string directly identifies them.
    # The model will learn what behavioral features (entropy, rate, timing) correlate
    # with these bots — without being told the entropy threshold explicitly.
    if session.get('headless_ua') or session.get('ai_bot_ua') or session.get('bot_ua'):
        return 1

    # --- Confident humans: passed an active challenge ---
    # A human who solved a challenge is a very reliable positive human label.
    if session.get('human') and session.get('passed_challenge'):
        return 0

    return None  # uncertain, exclude from training


class BaskervillehallClassifierTrainer:
    """
    Production trainer for the global XGBoost classifier (baskerville_score_4).
    Runs as a separate Kubernetes pod alongside baskervillehall_trainer.
    """

    HOST = 'global'  # model is global, not per-domain

    def __init__(
            self,
            topic_sessions='BASKERVILLEHALL_SESSIONS',
            group_id='classifier_train_pipeline',
            kafka_connection=None,
            s3_connection=None,
            s3_path='/',
            num_sessions=50000,
            min_dataset_size=1000,
            dataset_delay_from_now_in_minutes=2,
            model_ttl_in_minutes=240,
            wait_time_minutes=5,
            n_estimators=1000,
            kafka_timeout_ms=1000,
            kafka_max_size=200,
            validate_sample_size=5000,
            logger=None,
    ):
        if kafka_connection is None:
            kafka_connection = {'bootstrap_servers': 'localhost:9092'}
        if s3_connection is None:
            s3_connection = {}

        self.topic_sessions = topic_sessions
        self.group_id = group_id
        self.kafka_connection = kafka_connection
        self.s3_connection = s3_connection
        # Classifier models are stored under a separate S3 prefix
        self.s3_path = f'{s3_path}_classifier'
        self.num_sessions = num_sessions
        self.min_dataset_size = min_dataset_size
        self.dataset_delay_from_now_in_minutes = dataset_delay_from_now_in_minutes
        self.model_ttl_in_minutes = model_ttl_in_minutes
        self.wait_time_minutes = wait_time_minutes
        self.n_estimators = n_estimators
        self.kafka_timeout_ms = kafka_timeout_ms
        self.kafka_max_size = kafka_max_size
        self.validate_sample_size = validate_sample_size
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

        self._last_trained_at = None

    def _is_ttl_expired(self):
        if self._last_trained_at is None:
            return True
        elapsed_minutes = (time.time() - self._last_trained_at) / 60
        return elapsed_minutes >= self.model_ttl_in_minutes

    def _collect_sessions(self, consumer):
        """
        Seek Kafka topic to beginning and collect:
          - up to num_sessions labeled sessions (for training)
          - up to validate_sample_size unlabeled sessions (for post-training score distribution)
        Stops early if we reach messages newer than dataset_delay_from_now_in_minutes.
        """
        sessions = []
        labels = []
        unlabeled_sample = []
        time_now = int(time.time())
        skipped_recent = skipped_filter = skipped_unlabeled = 0

        consumer.seek_to_beginning()
        self.logger.info(f'Collecting up to {self.num_sessions} labeled sessions...')

        exhausted = False
        while (len(sessions) < self.num_sessions or len(unlabeled_sample) < self.validate_sample_size) \
                and not exhausted:
            raw_messages = consumer.poll(
                timeout_ms=self.kafka_timeout_ms,
                max_records=self.kafka_max_size,
            )
            if not raw_messages:
                self.logger.info('No more messages from Kafka — topic exhausted.')
                break

            for _, messages in raw_messages.items():
                if exhausted:
                    break
                for message in messages:
                    age_minutes = (time_now - message.timestamp / 1000) / 60
                    if age_minutes < self.dataset_delay_from_now_in_minutes:
                        self.logger.info('Reached current-time boundary, stopping collection.')
                        exhausted = True
                        break

                    if not message.value:
                        continue

                    session = json.loads(message.value.decode('utf-8'))

                    # Session-level filters
                    if session.get('asset_only', False):
                        skipped_filter += 1
                        continue
                    if session.get('primary_session', False):
                        skipped_filter += 1
                        continue
                    if session.get('verified_bot', False):
                        skipped_filter += 1
                        continue
                    if len(session.get('requests', [])) < 8:
                        skipped_filter += 1
                        continue

                    label = _label_session(session)
                    if label is None:
                        skipped_unlabeled += 1
                        if len(unlabeled_sample) < self.validate_sample_size:
                            unlabeled_sample.append(_strip_session(session))
                        continue

                    if len(sessions) < self.num_sessions:
                        sessions.append(_strip_session(session))
                        labels.append(label)

                    if len(sessions) >= self.num_sessions and len(unlabeled_sample) >= self.validate_sample_size:
                        exhausted = True
                        break

        n_bots = sum(labels)
        n_humans = len(labels) - n_bots
        self.logger.info(
            f'Collection done: {len(sessions)} labeled sessions '
            f'(bots={n_bots}, humans={n_humans}), '
            f'{len(unlabeled_sample)} unlabeled for validation. '
            f'Skipped: filter={skipped_filter}, unlabeled_total={skipped_unlabeled}, recent={skipped_recent}.'
        )
        return sessions, labels, unlabeled_sample

    def _validate_on_unlabeled(self, model, unlabeled_sample):
        """
        Score a sample of unlabeled (uncertain) sessions with the trained model and
        log the score distribution. This tells us how the model behaves in the grey zone
        — sessions that neither clearly flagged as bots nor confirmed as humans.
        """
        if not unlabeled_sample:
            return

        try:
            scores = model.predict_proba(unlabeled_sample)
            scores = np.array(scores)

            buckets = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.01]
            counts = np.histogram(scores, bins=buckets)[0]
            pct = counts / len(scores) * 100

            self.logger.info(
                f'Score distribution on {len(scores)} unlabeled (uncertain) sessions:'
            )
            labels_str = ['0.0-0.1', '0.1-0.2', '0.2-0.3', '0.3-0.4', '0.4-0.5',
                          '0.5-0.6', '0.6-0.7', '0.7-0.8', '0.8-0.9', '0.9-1.0']
            for label, p in zip(labels_str, pct):
                bar = '#' * int(p / 2)
                self.logger.info(f'  {label}: {p:5.1f}% {bar}')

            threshold = model.threshold
            above = (scores >= threshold).sum()
            self.logger.info(
                f'  => {above} / {len(scores)} ({above/len(scores)*100:.1f}%) '
                f'would be challenged (score >= threshold={threshold:.3f})'
            )
            p10, p25, p50, p75, p90 = np.percentile(scores, [10, 25, 50, 75, 90])
            self.logger.info(
                f'  Percentiles: p10={p10:.3f} p25={p25:.3f} p50={p50:.3f} '
                f'p75={p75:.3f} p90={p90:.3f}'
            )
        except Exception:
            self.logger.exception('Validation on unlabeled sessions failed (non-fatal).')

    def _train_and_save(self, sessions, labels, unlabeled_sample=None):
        """Train XGBoost classifier on labeled sessions and save to S3."""
        if len(sessions) < self.min_dataset_size:
            self.logger.warning(
                f'Too few labeled sessions: {len(sessions)} < {self.min_dataset_size}. Skipping training.'
            )
            return False

        n_bots = sum(labels)
        n_humans = len(labels) - n_bots
        self.logger.info(
            f'Training classifier on {len(sessions)} sessions '
            f'(bots={n_bots} / {n_bots/len(labels)*100:.1f}%, '
            f'humans={n_humans} / {n_humans/len(labels)*100:.1f}%)'
        )

        model = BaskervilleClassifier(
            n_estimators=self.n_estimators,
            logger=self.logger,
        )

        model.fit(sessions, np.array(labels))

        self.logger.info(
            f'Classifier trained: AUC-PR={model.aucpr_test:.4f}, '
            f'threshold={model.threshold:.3f}, F1={model.best_f1:.3f}, '
            f'best_round={model.best_round}'
        )

        if unlabeled_sample:
            self._validate_on_unlabeled(model, unlabeled_sample)

        model.clear_embeddings()
        model_io = ModelIO(**self.s3_connection, logger=self.logger)
        model_io.save(model, self.s3_path, self.HOST, model_type=ModelType.GENERIC)
        self.logger.info(f'Classifier model saved to {self.s3_path}/{self.HOST}/generic')

        del model
        gc.collect()
        return True

    def run(self):
        consumer_opts = dict(self.kafka_connection)
        consumer_opts.update({
            'fetch_max_bytes': 16 * 1024 * 1024,
            'max_partition_fetch_bytes': 4 * 1024 * 1024,
            'fetch_min_bytes': 1024 * 1024,
            'fetch_max_wait_ms': 500,
            'receive_buffer_bytes': 4 * 1024 * 1024,
            'send_buffer_bytes': 1024 * 1024,
            'enable_auto_commit': True,
            'request_timeout_ms': 60000,
            'session_timeout_ms': 20000,
            'group_id': self.group_id,
            'auto_offset_reset': 'earliest',
        })

        consumer = KafkaConsumer(**consumer_opts)
        consumer.subscribe([self.topic_sessions])

        # Wait up to 30s for partition assignment
        start = time_module.time()
        while not consumer.assignment():
            consumer.poll(timeout_ms=1000)
            if time_module.time() - start > 30:
                break
        self.logger.info(f'Assigned partitions: {consumer.assignment()}')
        self.logger.info(
            f'Classifier trainer started. topic={self.topic_sessions}, '
            f'ttl={self.model_ttl_in_minutes}min, target_sessions={self.num_sessions}'
        )

        while True:
            try:
                if not self._is_ttl_expired():
                    remaining = self.model_ttl_in_minutes - (time.time() - self._last_trained_at) / 60
                    self.logger.info(
                        f'Model TTL not expired. Next training in {remaining:.1f} min. '
                        f'Sleeping {self.wait_time_minutes} min...'
                    )
                    time.sleep(self.wait_time_minutes * 60)
                    continue

                self.logger.info('TTL expired — starting classifier training cycle.')
                sessions, labels, unlabeled_sample = self._collect_sessions(consumer)

                success = self._train_and_save(sessions, labels, unlabeled_sample)
                del sessions, labels, unlabeled_sample
                gc.collect()

                if success:
                    self._last_trained_at = time.time()
                    self.logger.info(
                        f'Training successful. Next training in {self.model_ttl_in_minutes} min.'
                    )
                else:
                    self.logger.warning(
                        f'Training skipped. Retrying in {self.wait_time_minutes} min.'
                    )
                    time.sleep(self.wait_time_minutes * 60)

            except Exception:
                self.logger.exception('Exception in classifier trainer loop. Retrying...')
                time.sleep(self.wait_time_minutes * 60)
