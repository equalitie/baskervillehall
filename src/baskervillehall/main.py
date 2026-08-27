import argparse
import logging
import os
import json

logger = None


def get_logger(
        name, logging_level=logging.DEBUG
):
    '''
    Creates a logger that logs to file and console with the same logging level
    :param str name: the logger name
    :param int logging_level: the logging level
    :param str output_file: the file to save to
    :return: the initialized logger
    :rtype: logger
    '''
    logger = logging.getLogger(name)
    logger.setLevel(logging_level)

    # create formatter and add it to the handlers
    formatter = logging.Formatter(
        '%(asctime)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-8s'
        ' [%(process)d] %(message)s'
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger


def main():
    '''
    Baskervillehall commandline arguments
    :return:
    '''
    logger = get_logger(
        __name__,
        logging_level=logging.INFO
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'pipeline',
        help='Pipeline to use: train, predict',
    )
    args = parser.parse_args()

    kafka_connection = {
        'bootstrap_servers': os.environ.get('BOOTSTRAP_SERVERS')
    }

    kafka_connection_output = {
        'bootstrap_servers': os.environ.get('BOOTSTRAP_SERVERS_OUTPUT', '')
    }

    debug_ip = os.environ.get('DEBUG_IP')

    s3_connection = {
        's3_endpoint': os.environ.get('S3_ENDPOINT'),
        's3_access': os.environ.get('S3_ACCESS'),
        's3_secret': os.environ.get('S3_SECRET'),
        's3_region': os.environ.get('S3_REGION'),
    }

    postgres_connection = {
        'host': os.environ.get('POSTGRES_HOST'),
        'user': os.environ.get('POSTGRES_USER'),
        'password': os.environ.get('POSTGRES_PASSWORD'),
        'database': os.environ.get('POSTGRES_DATABASE_NAME'),
        'port': int(os.environ.get('POSTGRES_PORT'))
    }

    if args.pipeline == 'train':
        from baskervillehall.baskervillehall_trainer import BaskervillehallTrainer
        params = {
            'accepted_contamination': float(os.environ.get('ACCEPTED_CONTAMINATION')),
            'features': os.environ.get('FEATURES').split(','),
            'categorical_features': os.environ.get('CATEGORICAL_FEATURES').split(','),
            'pca_feature': os.environ.get('PCA_FEATURE') == 'True',
            'max_categories': int(os.environ.get('MAX_CATEGORIES')),
            'min_category_frequency': int(os.environ.get('MIN_CATEGORY_FREQUENCY')),
            'topic_sessions': os.environ.get('TOPIC_SESSIONS'),
            'group_id': os.environ.get('GROUP_ID_TRAIN', 'train_pipeline'),
            'train_batch_size': int(os.environ.get('TRAIN_BATCH_SIZE')),
            'num_sessions': int(os.environ.get('NUM_SESSIONS')),
            'min_session_duration': int(os.environ.get('MIN_SESSION_DURATION')),
            'min_number_of_requests': int(os.environ.get('MIN_NUMBER_OF_REQUESTS')),
            'n_estimators': int(os.environ.get('N_ESTIMATORS')),
            'max_samples': int(os.environ.get('MAX_SAMPLES')),
            'contamination': float(os.environ.get('CONTAMINATION')),
            'max_features': float(os.environ.get('MAX_FEATURES')),
            'bootstrap': os.environ.get('BOOTSTRAP') == 'True',
            'random_state': int(os.environ.get('RANDOM_STATE')),
            'model_ttl_in_minutes': int(os.environ.get('MODEL_TTL_IN_MINUTES')),
            'dataset_delay_from_now_in_minutes': int(os.environ.get('DATASET_DELAY_FROM_NOW_IN_MINUTES')),
            's3_path': os.environ.get('S3_MODEL_STORAGE_PATH'),
            'min_dataset_size': int(os.environ.get('MIN_DATASET_SIZE')),
            'small_dataset_size': int(os.environ.get('SMALL_DATASET_SIZE')),
            'wait_time_minutes': int(os.environ.get('TRAINER_WAIT_TIME_MINUTES')),
            'datetime_format': os.environ.get('DATETIME_FORMAT'),
            'n_jobs': int(os.environ.get('N_JOBS'))
        }
        trainer = BaskervillehallTrainer(
            **params,
            kafka_connection=kafka_connection,
            s3_connection=s3_connection,
            logger=logger
        )
        trainer.run()
    elif args.pipeline == 'classifier_train':
        from baskervillehall.baskervillehall_classifier_trainer import BaskervillehallClassifierTrainer
        params = {
            'topic_sessions': os.environ.get('TOPIC_SESSIONS'),
            'group_id': os.environ.get('GROUP_ID_CLASSIFIER_TRAIN', 'classifier_train_pipeline'),
            'num_sessions': int(os.environ.get('CLASSIFIER_NUM_SESSIONS', 50000)),
            'min_dataset_size': int(os.environ.get('CLASSIFIER_MIN_DATASET_SIZE', 1000)),
            'dataset_delay_from_now_in_minutes': int(os.environ.get('CLASSIFIER_DATASET_DELAY_MINUTES', 2)),
            'model_ttl_in_minutes': int(os.environ.get('CLASSIFIER_MODEL_TTL_MINUTES', 240)),
            'wait_time_minutes': int(os.environ.get('CLASSIFIER_WAIT_TIME_MINUTES', 5)),
            'n_estimators': int(os.environ.get('CLASSIFIER_N_ESTIMATORS', 1000)),
            's3_path': os.environ.get('S3_MODEL_STORAGE_PATH'),
        }
        trainer = BaskervillehallClassifierTrainer(
            **params,
            kafka_connection=kafka_connection,
            s3_connection=s3_connection,
            logger=logger
        )
        trainer.run()
    elif args.pipeline == 'predict':
        from baskervillehall.baskervillehall_predictor import BaskervillehallPredictor
        params = {
            'topic_sessions': os.environ.get('TOPIC_SESSIONS'),
            'group_id': os.environ.get('GROUP_ID_PREDICTOR', 'predict_pipeline'),
            'topic_commands': os.environ.get('TOPIC_COMMANDS'),
            'topic_commands_output': os.environ.get('TOPIC_COMMANDS_OUTPUT'),
            'topic_reports': os.environ.get('TOPIC_REPORTS'),
            'model_reload_in_minutes': int(os.environ.get('PREDICTOR_MODEL_RELOAD_IN_MINUTES')),
            'min_session_duration': int(os.environ.get('MIN_SESSION_DURATION')),
            'min_number_of_requests': int(os.environ.get('MIN_NUMBER_OF_REQUESTS')),
            'num_offences_for_difficult_challenge': int(os.environ.get('NUM_OFFENCES_FOR_DIFFICULT_CHALLENGE')),
            'batch_size': int(os.environ.get('BATCH_SIZE')),
            's3_path': os.environ.get('S3_MODEL_STORAGE_PATH'),
            'pending_ttl': int(os.environ.get('PENDING_TTL')),
            'maxsize_pending': int(os.environ.get('MAXSIZE_PENDING')),
            'datetime_format': os.environ.get('DATETIME_FORMAT'),
            'n_jobs_predict': int(os.environ.get('N_JOBS_PREDICT')),
            'deflect_config_url': os.environ.get('DEFLECT_CONFIG_URL'),
            'deflect_config_auth': os.environ.get('DEFLECT_CONFIG_AUTH'),
            'white_list_refresh_period': int(os.environ.get('WHITELIST_REFRESH_IN_MINUTES', 5)),
            'ip_whitelist_url': os.environ.get('DEFLECT_CONFIG_URL_IP_WHITELIST', None),
            'ip_whitelist_auth': os.environ.get('DEFLECT_CONFIG_AUTH_IP_WHITELIST', None),
            'global_allowlist_url': os.environ.get('DEFLECT_CONFIG_URL_GLOBAL_ALLOWLIST', None),
            'global_allowlist_auth': os.environ.get('DEFLECT_CONFIG_AUTH_GLOBAL_ALLOWLIST', None),
            'origin_ips_url': os.environ.get('DEFLECT_CONFIG_URL_ORIGIN_IPS', None),
            'origin_ips_auth': os.environ.get('DEFLECT_CONFIG_AUTH_ORIGIN_IPS', None),
            'bad_bot_challenge': os.environ.get('BAD_BOT_CHALLENGE') == 'True',
            'use_shapley': os.environ.get('USE_SHAPLEY') == 'True',
            'postgres_connection': postgres_connection,
            'postgres_refresh_period_in_seconds': int(os.environ.get('POSTGRES_REFRESH_PERIOD_IN_SECONDS')),
            'sensitivity_factor': float(os.environ.get('SENSITIVITY_FACTOR')),
            'max_sessions_for_ip': float(os.environ.get('MAX_SESSIONS_FOR_IP')),
            'bot_score_threshold': float(os.environ.get('BOT_SCORE_THRESHOLD', 0.8)),
            'challenge_scrapers': os.environ.get('CHALLENGE_SCRAPERS') == 'True',
            'block_commercial_crawlers': os.environ.get('BLOCK_COMMERCIAL_CRAWLERS', 'True') == 'True',
            'worker_chunk_size': int(os.environ.get('WORKER_CHUNK_SIZE', 1000)),
            'kafka_poll_timeout_ms': int(os.environ.get('KAFKA_POLL_TIMEOUT_MS', 5000)),
            'max_poll_interval_ms': int(os.environ.get('MAX_POLL_INTERVAL_MS', 600000)),
            'fetch_max_wait_ms': int(os.environ.get('FETCH_MAX_WAIT_MS', 2000)),
            'fetch_min_bytes': int(os.environ.get('FETCH_MIN_BYTES', 1048576)),
            'lag_high_threshold': int(os.environ.get('LAG_HIGH_THRESHOLD', 10000)),
            'lag_moderate_threshold': int(os.environ.get('LAG_MODERATE_THRESHOLD', 5000)),
            'use_rate_limit': os.environ.get('USE_RATE_LIMIT', 'False') == 'True',
            'rate_limit_hits': int(os.environ.get('RATE_LIMIT_HITS', 20)),
            'rate_limit_interval': int(os.environ.get('RATE_LIMIT_INTERVAL', 60)),
            'rate_limit_expiration': int(os.environ.get('RATE_LIMIT_EXPIRATION', 300)),
            'dnet_partition_map': json.loads(os.environ.get('DNET_PARTITION_MAP', '{}')),
            'print_log_in_command': os.environ.get('PRINT_LOG_IN_COMMAND') == 'True',
            'use_baskerville_score': os.environ.get('USE_BASKERVILLE_SCORE', 'False') == 'True',
            'verbose_classifier': os.environ.get('VERBOSE_CLASSIFIER', 'False') == 'True',
            'attack_response_mode': os.environ.get('ATTACK_RESPONSE_MODE', 'True') == 'True',
            'attack_min_challenge_count': int(os.environ.get('ATTACK_MIN_CHALLENGE_COUNT', 50)),
            'attack_min_spike_ratio': float(os.environ.get('ATTACK_MIN_SPIKE_RATIO', 4.0)),
            'attack_aggressive_spike_ratio': float(os.environ.get('ATTACK_AGGRESSIVE_SPIKE_RATIO', 6.0)),
            'attack_extreme_spike_ratio': float(os.environ.get('ATTACK_EXTREME_SPIKE_RATIO', 15.0)),
            'session_llm_enabled': os.environ.get('SESSION_LLM_ENABLED', 'True') == 'True',
            'session_llm_provider': os.environ.get('SESSION_LLM_PROVIDER', 'ollama'),
            'ollama_url': os.environ.get('OLLAMA_URL', 'http://localhost:11434'),
            'llm_model': os.environ.get('SESSION_LLM_MODEL', os.environ.get('LLM_MODEL', 'qwen2.5:7b')),
            'openai_api_key': os.environ.get('ANTHROPIC_API_KEY', os.environ.get('OPENAI_API_KEY', '')),
            'session_llm_score_min': int(os.environ.get('SESSION_LLM_SCORE_MIN', '20')),
            'session_llm_score_max': int(os.environ.get('SESSION_LLM_SCORE_MAX', '80')),
            'session_llm_min_requests': int(os.environ.get('SESSION_LLM_MIN_REQUESTS', '5')),
            'session_llm_queue_size': int(os.environ.get('SESSION_LLM_QUEUE_SIZE', '200')),
        }

        predictor = BaskervillehallPredictor(
            **params,
            kafka_connection=kafka_connection,
            kafka_connection_output=kafka_connection_output,
            s3_connection=s3_connection,
            debug_ip=debug_ip,
            hostname=os.environ.get('MY_POD_NAME', 'SESSION_POD'),
            logger=logger
        )
        predictor.run()
    elif args.pipeline == 'respond':
        from baskervillehall.incident_first_responder import IncidentFirstResponder
        llm_provider = os.environ.get('FIRST_RESPONDER_LLM_PROVIDER', 'ollama')
        IncidentFirstResponder(
            postgres_connection=postgres_connection,
            ollama_url=os.environ.get('OLLAMA_URL', 'http://localhost:11434'),
            llm_model=os.environ.get('LLM_MODEL', 'qwen2.5:7b'),
            llm_provider=llm_provider,
            openai_api_key=os.environ.get('ANTHROPIC_API_KEY', os.environ.get('OPENAI_API_KEY', '')),
            openai_model=os.environ.get('FIRST_RESPONDER_LLM_MODEL', 'claude-haiku-4-5-20251001'),
            check_interval=int(os.environ.get('FIRST_RESPONDER_CHECK_INTERVAL', '30')),
            min_attack_pct=float(os.environ.get('FIRST_RESPONDER_MIN_ATTACK_PCT', '50.0')),
            max_normal_pct=float(os.environ.get('FIRST_RESPONDER_MAX_NORMAL_PCT', '20.0')),
            ttl_minutes=int(os.environ.get('FIRST_RESPONDER_TTL_MINUTES', '30')),
            min_spike_ratio=float(os.environ.get('FIRST_RESPONDER_MIN_SPIKE_RATIO', '3.0')),
            min_challenge_count=int(os.environ.get('FIRST_RESPONDER_MIN_CHALLENGE_COUNT', '50')),
            kafka_connection=kafka_connection,
            kafka_connection_output=kafka_connection_output,
            topic_commands=os.environ.get('TOPIC_COMMANDS', 'banjax_command_topic'),
            dnet_partition_map=json.loads(os.environ.get('DNET_PARTITION_MAP', '{}')),
            fingerprint_min_pct=float(os.environ.get('FIRST_RESPONDER_FINGERPRINT_MIN_PCT', '40.0')),
            fingerprint_max_ips=int(os.environ.get('FIRST_RESPONDER_FINGERPRINT_MAX_IPS', '500')),
            host_whitelist=[h for h in os.environ.get('FIRST_RESPONDER_HOST_WHITELIST', '').split(',') if h.strip()],
            min_traffic_peak_count=int(os.environ.get('FIRST_RESPONDER_MIN_TRAFFIC_PEAK_COUNT', '50')),
            min_traffic_peak_req_min=int(os.environ.get('FIRST_RESPONDER_MIN_TRAFFIC_PEAK_REQ_MIN', '50')),
            min_incident_duration_minutes=int(os.environ.get('FIRST_RESPONDER_MIN_INCIDENT_DURATION_MINUTES', '10')),
            logger=logger,
        ).run()
    else:
        logger.error(f'Pipeline "{args.pipeline}" is not supported.')

    logger.info('Pipeline finished.')
    exit()


if __name__ == '__main__':
    main()


