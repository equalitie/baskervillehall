import argparse
import logging

import os

from baskervillehall.baskerville_pinger import BaskervillePinger
from baskervillehall.baskervillehall_counter import BaskervillehallCounter
from baskervillehall.baskervillehall_predictor import BaskervillehallPredictor
from baskervillehall.baskervillehall_session import BaskervillehallSession
from baskervillehall.baskervillehall_trainer import BaskervillehallTrainer

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

    s3_connection = {
        's3_endpoint': os.environ.get('S3_ENDPOINT'),
        's3_access': os.environ.get('S3_ACCESS'),
        's3_secret': os.environ.get('S3_SECRET'),
        's3_region': os.environ.get('S3_REGION'),
    }

    whitelist_url_default = os.environ.get('WHITELIST_URL_DEFAULT').split(',')

    if args.pipeline == 'session':
        session_parameters = {
            'topic_weblogs': os.environ.get('TOPIC_WEBLOGS'),
            'topic_sessions': os.environ.get('TOPIC_SESSIONS'),
            'session_inactivity': int(os.environ.get('SESSION_INACTIVITY')),
            'flush_window_seconds': int(os.environ.get('FLUSH_WINDOW_SECONDS')),
            'garbage_collection_period': int(os.environ.get('GARBAGE_COLLECTION_PERIOD')),
            'partition': partition,
            'kafka_group_id': os.environ.get('GROUP_ID_SESSION'),
            'max_fresh_sessions_per_ip': int(os.environ.get('MAX_FRESH_SESSIONS_PER_IP')),
            'fresh_session_ttl_minutes': int(os.environ.get('FRESH_SESSION_TTL_MINUTES')),
            'ip_fresh_sessions_limit': int(os.environ.get('IP_FRESH_SESSIONS_LIMIT')),
            'fresh_session_grace_period': int(os.environ.get('FRESH_SESSION_GRACE_PERIOD')),
            'datetime_format': os.environ.get('DATETIME_FORMAT')
        }
        sessionizer = BaskervillehallSession(
            **session_parameters,
            kafka_connection=kafka_connection,
            logger=logger
        )
        sessionizer.run()

    elif args.pipeline == 'train':
        trainer_parameters = {
            'feature_names': os.environ.get('FEATURE_NAMES').split(','),
            'topic_sessions': os.environ.get('TOPIC_SESSIONS'),
            'partition': partition,
            'train_batch_size': int(os.environ.get('TRAIN_BATCH_SIZE')),
            'num_sessions': int(os.environ.get('NUM_SESSIONS')),
            'min_session_duration': int(os.environ.get('MIN_SESSION_DURATION')),
            'min_number_of_queries': int(os.environ.get('MIN_NUMBER_OF_QUERIES')),
            'n_estimators': int(os.environ.get('N_ESTIMATORS')),
            'max_samples': int(os.environ.get('MAX_SAMPLES')),
            'contamination': float(os.environ.get('CONTAMINATION')),
            'max_features': float(os.environ.get('MAX_FEATURES')),
            'random_state': int(os.environ.get('RANDOM_STATE')),
            'model_ttl_in_minutes': int(os.environ.get('MODEL_TTL_IN_MINUTES')),
            'dataset_delay_from_now_in_minutes': int(os.environ.get('DATASET_DELAY_FROM_NOW_IN_MINUTES')),
            's3_path': os.environ.get('S3_MODEL_STORAGE_PATH'),
            'min_dataset_size': int(os.environ.get('MIN_DATASET_SIZE')),
            'small_dataset_size': int(os.environ.get('SMALL_DATASET_SIZE')),
            'kafka_group_id': os.environ.get('GROUP_ID_TRAINER'),
            'wait_time_minutes': int(os.environ.get('TRAINER_WAIT_TIME_MINUTES')),
            'datetime_format': os.environ.get('DATETIME_FORMAT')
        }
        trainer = BaskervillehallTrainer(
            **trainer_parameters,
            kafka_connection=kafka_connection,
            s3_connection=s3_connection,
            logger=logger
        )
        trainer.run()
    elif args.pipeline == 'predict':
        predictor_parameters = {
            'topic_sessions': os.environ.get('TOPIC_SESSIONS'),
            'partition': partition,
            'num_partitions': int(os.environ.get('NUM_PARTITIONS')),
            'topic_commands': os.environ.get('TOPIC_COMMANDS'),
            'topic_reports': os.environ.get('TOPIC_REPORTS'),
            'kafka_group_id': os.environ.get('GROUP_ID_PREDICTOR'),
            'model_reload_in_minutes': int(os.environ.get('PREDICTOR_MODEL_RELOAD_IN_MINUTES')),
            'min_session_duration': int(os.environ.get('MIN_SESSION_DURATION')),
            'min_number_of_queries': int(os.environ.get('MIN_NUMBER_OF_QUERIES')),
            'batch_size': int(os.environ.get('BATCH_SIZE')),
            's3_path': os.environ.get('S3_MODEL_STORAGE_PATH'),
            'white_list_refresh_in_minutes': int(os.environ.get('WHITELIST_REFRESH_IN_MINUTES')),
            'whitelist_ip': os.environ.get('WHITELIST_IP'),
            'pending_challenge_ttl_in_minutes': int(os.environ.get('PENDING_CHALLENGE_TTL_IN_MINUTES')),
            'passed_challenge_ttl_in_minutes': int(os.environ.get('PASSED_CHALLENGE_TTL_IN_MINUTES')),
            'maxsize_passed_challenge': int(os.environ.get('MAXSIZE_PASSED_CHALLENGE')),
            'maxsize_pending_challenge': int(os.environ.get('MAXSIZE_PENDING_CHALLENGE')),
            'datetime_format': os.environ.get('DATETIME_FORMAT')
        }

        predictor = BaskervillehallPredictor(
            **predictor_parameters,
            kafka_connection=kafka_connection,
            s3_connection=s3_connection,
            logger=logger
        )
        predictor.run()
    elif args.pipeline == 'counter':
        kafka_connection = {
            'bootstrap_servers': os.environ.get('BOOTSTRAP_SERVERS_COUNTER')
        }
        counter_parameters = {
            'topic': os.environ.get('TOPIC_COUNTER'),
            'partition': partition,
            'kafka_group_id': os.environ.get('GROUP_ID_COUNTER'),
            'batch_size': int(os.environ.get('BATCH_SIZE')),
            'window': int(os.environ.get('WINDOW_COUNTER'))
        }

        counter = BaskervillehallCounter(
            **counter_parameters,
            kafka_connection=kafka_connection,
            logger=logger
        )
        counter.run()
    elif args.pipeline == 'pinger':
        kafka_connection = {
            'bootstrap_servers': os.environ.get('PING_BOOTSTRAP_SERVERS')
        }
        pinger_parameters = {
            'topic': os.environ.get('PING_TOPIC'),
            'topic_output': os.environ.get('PING_TOPIC_OUTPUT'),
            'partition': partition,
            'kafka_group_id': os.environ.get('PING_GROUP_ID'),
            'num_workers': int(os.environ.get('PING_NUM_WORKERS')),
            'host_refresh_minutes': int(os.environ.get('PING_HOST_REFRESH_MINUTES'))
        }

        counter = BaskervillePinger(
            **pinger_parameters,
            kafka_connection=kafka_connection,
            logger=logger
        )
        counter.run()
    else:
        logger.error('Pipeline is not specified. Use session, predict or train')


if __name__ == '__main__':
    main()


