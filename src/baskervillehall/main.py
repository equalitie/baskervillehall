import argparse
import logging

import os

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
            'secondary_validation_period': int(os.environ.get('SECONDARY_VALIDATION_PERIOD')),
            'garbage_collection_period': int(os.environ.get('GARBAGE_COLLECTION_PERIOD')),
            'partition': partition,
            'kafka_group_id': os.environ.get('GROUP_ID_SESSION'),
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
            'num_sessions': int(os.environ.get('NUM_SESSIONS')),
            'min_session_duration': int(os.environ.get('MIN_SESSION_DURATION')),
            'min_number_of_queries': int(os.environ.get('MIN_NUMBER_OF_QUERIES')),
            'n_estimators': int(os.environ.get('N_ESTIMATORS')),
            'max_samples': int(os.environ.get('MAX_SAMPLES')),
            'contamination': float(os.environ.get('CONTAMINATION')),
            'max_features': float(os.environ.get('MAX_FEATURES')),
            'random_state': int(os.environ.get('RANDOM_STATE')),
            'host_waiting_sleep_time_in_seconds': int(os.environ.get('HOST_WAITING_SLEEP_TIME_IN_SECONDS')),
            'model_ttl_in_minutes': int(os.environ.get('MODEL_TTL_IN_MINUTES')),
            'dataset_delay_from_now_in_minutes': int(os.environ.get('DATASET_DELAY_FROM_NOW_IN_MINUTES')),
            's3_path': os.environ.get('S3_MODEL_STORAGE_PATH'),
            'min_dataset_size': int(os.environ.get('MIN_DATASET_SIZE')),                                                                                                                                                                                                                                                                                     'kafka_group_id': os.environ.get('GROUP_ID_TRAINER'),
        }
        trainer = BaskervillehallTrainer(
            **trainer_parameters,
            kafka_connection=kafka_connection,
            logger=logger
        )
        trainer.run()
    elif args.pipeline == 'predict':
        pass
    else:
        logger.error('Pipeline is not specified. Use session, predict or train')


if __name__ == '__main__':
    main()


