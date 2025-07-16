import argparse
import logging

import os

from baskervillehall.alert_challenge_rate import AlertChallengeRate
from baskervillehall.storage_commands import StorageCommands
from baskervillehall.baskervillehall_predictor import BaskervillehallPredictor
from baskervillehall.baskervillehall_session import BaskervillehallSession
from baskervillehall.baskervillehall_trainer import BaskervillehallTrainer
from baskervillehall.storage_sessions import StorageSessions

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

    partition = int(os.environ.get('MY_POD_NAME').split('-')[-1])
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

    if args.pipeline == 'session':
        params = {
            'read_from_beginning': os.environ.get('READ_FROM_BEGINNING') == 'True',
            'flush_increment': int(os.environ.get('FLUSH_INCREMENT')),
            'topic_weblogs': os.environ.get('TOPIC_WEBLOGS'),
            'topic_sessions': os.environ.get('TOPIC_SESSIONS'),
            'session_inactivity': int(os.environ.get('SESSION_INACTIVITY')),
            'garbage_collection_period': int(os.environ.get('GARBAGE_COLLECTION_PERIOD')),
            'partition': partition,
            'min_session_duration': int(os.environ.get('MIN_SESSION_DURATION')),
            'max_session_duration': int(os.environ.get('MAX_SESSION_DURATION')),
            'primary_session_expiration': int(os.environ.get('PRIMARY_SESSION_EXPIRATION')),
            'max_primary_sessions_per_ip': int(os.environ.get('MAX_PRIMARY_SESSIONS_PER_IP')),
            'datetime_format': os.environ.get('DATETIME_FORMAT'),
            'min_number_of_requests': int(os.environ.get('MIN_NUMBER_OF_REQUESTS')),
            'deflect_config_url': os.environ.get('DEFLECT_CONFIG_URL'),
            'whitelist_url_default': os.environ.get('WHITELIST_URL_DEFAULT').split(','),
            'postgres_connection': postgres_connection
        }

        sessionizer = BaskervillehallSession(
            **params,
            kafka_connection=kafka_connection,
            debug_ip=debug_ip,
            asn_database_path=os.environ.get('BAD_ASN_FILE'),
            asn_database2_path=os.environ.get('VPN_ASN_PATH'),
            logger=logger
        )
        sessionizer.run()

    elif args.pipeline == 'train':
        params = {
            'accepted_contamination': float(os.environ.get('ACCEPTED_CONTAMINATION')),
            'features': os.environ.get('FEATURES').split(','),
            'categorical_features': os.environ.get('CATEGORICAL_FEATURES').split(','),
            'pca_feature': os.environ.get('PCA_FEATURE') == 'True',
            'max_categories': int(os.environ.get('MAX_CATEGORIES')),
            'min_category_frequency': int(os.environ.get('MIN_CATEGORY_FREQUENCY')),
            'topic_sessions': os.environ.get('TOPIC_SESSIONS'),
            'partition': partition,
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
            'n_jobs': int(os.environ.get('N_JOBS')),
            'single_model': os.environ.get('SINGLE_MODEL') == 'True'
        }
        trainer = BaskervillehallTrainer(
            **params,
            kafka_connection=kafka_connection,
            s3_connection=s3_connection,
            logger=logger
        )
        trainer.run()
    elif args.pipeline == 'predict':
        params = {
            'topic_sessions': os.environ.get('TOPIC_SESSIONS'),
            'partition': partition,
            'num_partitions': int(os.environ.get('NUM_PARTITIONS')),
            'topic_commands': os.environ.get('TOPIC_COMMANDS'),
            'topic_reports': os.environ.get('TOPIC_REPORTS'),
            'model_reload_in_minutes': int(os.environ.get('PREDICTOR_MODEL_RELOAD_IN_MINUTES')),
            'min_session_duration': int(os.environ.get('MIN_SESSION_DURATION')),
            'min_number_of_requests': int(os.environ.get('MIN_NUMBER_OF_REQUESTS')),
            'num_offences_for_difficult_challenge': int(os.environ.get('NUM_OFFENCES_FOR_DIFFICULT_CHALLENGE')),
            'batch_size': int(os.environ.get('BATCH_SIZE')),
            's3_path': os.environ.get('S3_MODEL_STORAGE_PATH'),
            'whitelist_ip': os.environ.get('WHITELIST_IP'),
            'pending_ttl': int(os.environ.get('PENDING_TTL')),
            'maxsize_pending': int(os.environ.get('MAXSIZE_PENDING')),
            'datetime_format': os.environ.get('DATETIME_FORMAT'),
            'n_jobs_predict': int(os.environ.get('N_JOBS_PREDICT')),
            'deflect_config_url': os.environ.get('DEFLECT_CONFIG_URL'),
            'bad_bot_challenge': os.environ.get('BAD_BOT_CHALLENGE') == 'True',
            'use_shapley': os.environ.get('USE_SHAPLEY') == 'True',
            'postgres_connection': None, #postgres_connection,
            'postgres_refresh_period_in_seconds': int(os.environ.get('POSTGRES_REFRESH_PERIOD_IN_SECONDS')),
            'sensitivity_factor': float(os.environ.get('SENSITIVITY_FACTOR')),
            'max_sessions_for_ip': float(os.environ.get('MAX_SESSIONS_FOR_IP')),
            'single_model': os.environ.get('SINGLE_MODEL') == 'True',
            'bot_score_threshold': float(os.environ.get('BOT_SCORE_THRESHOLD')),
            'challenge_scrapers': os.environ.get('CHALLENGE_SCRAPERS') == 'True'
        }

        predictor = BaskervillehallPredictor(
            **params,
            kafka_connection=kafka_connection,
            s3_connection=s3_connection,
            debug_ip=debug_ip,
            logger=logger
        )
        predictor.run()
    elif args.pipeline == 'storage':
        params = {
            'partition': partition,
            'batch_size': int(os.environ.get('BATCH_SIZE')),
            'datetime_format': os.environ.get('DATETIME_FORMAT'),
            'postgres_connection': postgres_connection,
            'ttl_records_days': int(os.environ.get('TTL_RECORDS_DAYS')),
            'autocreate_hostname_id': os.environ.get('AUTOCREATE_HOSTNAME_ID') == 'True'
        }
        num_requests = int(os.environ.get("NUM_REQUESTS_IN_STORAGE"))

        storage_sessions = StorageSessions(
            **params,
            topic=os.environ.get('TOPIC_SESSIONS'),
            kafka_connection=kafka_connection,
            num_requests=num_requests,
            table=os.environ.get('SQL_TABLE_SESSIONS'),
            logger=logger
        )
        t1 = storage_sessions.start()

        storage_commands = StorageCommands(
            **params,
            topic=os.environ.get('TOPIC_COMMANDS'),
            kafka_connection=kafka_connection,
            num_requests=num_requests,
            table=os.environ.get('SQL_TABLE_COMMANDS'),
            logger=logger
        )
        t2 = storage_commands.start()

        if partition == 0:
            alert = AlertChallengeRate(
                postgres_connection=postgres_connection,
                aggregation_window_in_minutes=int(os.environ.get('ALERT_AGGREGATION_WINDOW_IN_MINUTES')),
                dataset_in_hours=int(os.environ.get('ALERT_DATASET_IN_HOURS')),
                zscore_hits=float(os.environ.get('ALERT_ZSCORE_HITS')),
                zscore_challenge_rate=float(os.environ.get('ALERT_ZSCORE_CHALLENGE_RATE')),
                pending_period_in_minutes=int(os.environ.get('ALERT_PENDING_PERIOD_IN_MINUTES')),
                min_num_sessions=int(os.environ.get('ALERT_MIN_NUM_SESSIONS')),
                host_white_list=os.environ.get('ALERT_HOST_WHITE_LIST').split(','),
                webhook=os.environ.get('ALERT_WEBHOOK'),
                cc=os.environ.get('ALERT_CC'),
                threshold_challenge_rate=int(os.environ.get('ALERT_THRESHOLD_CHALLENGE_RATE')),
                logger=logger
            )
            t3 = alert.start()
            t3.join()

        t1.join()
        t2.join()
    else:
        logger.error(f'Pipeline "{args.pipeline}" is not supported.')

    logger.info('Pipeline finished.')
    exit()


if __name__ == '__main__':
    main()


