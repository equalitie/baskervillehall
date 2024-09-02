import argparse
import logging

import os

from baskervillehall.baskervillehall_predictor import BaskervillehallPredictor
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
    debug_ip = os.environ.get('DEBUG_IP')

    s3_connection = {
        's3_endpoint': os.environ.get('S3_ENDPOINT'),
        's3_access': os.environ.get('S3_ACCESS'),
        's3_secret': os.environ.get('S3_SECRET'),
        's3_region': os.environ.get('S3_REGION'),
    }

    if args.pipeline == 'session':
        session_parameters = {
            'read_from_beginning': os.environ.get('READ_FROM_BEGINNING') == 'True',
            'flush_increment': int(os.environ.get('FLUSH_INCREMENT')),
            'topic_weblogs': os.environ.get('TOPIC_WEBLOGS'),
            'topic_sessions': os.environ.get('TOPIC_SESSIONS'),
            'session_inactivity': int(os.environ.get('SESSION_INACTIVITY')),
            'garbage_collection_period': int(os.environ.get('GARBAGE_COLLECTION_PERIOD')),
            'partition': partition,
            'min_session_duration': int(os.environ.get('MIN_SESSION_DURATION')),
            'max_session_duration': int(os.environ.get('MAX_SESSION_DURATION')),
            'max_primary_sessions_per_ip': int(os.environ.get('MAX_PRIMARY_SESSIONS_PER_IP')),
            'datetime_format': os.environ.get('DATETIME_FORMAT'),
            'min_number_of_requests': int(os.environ.get('MIN_NUMBER_OF_REQUESTS')),
            'whitelist_url': os.environ.get('WHITELIST_URL'),
            'whitelist_url_default': os.environ.get('WHITELIST_URL_DEFAULT').split(',')
        }
        sessionizer = BaskervillehallSession(
            **session_parameters,
            kafka_connection=kafka_connection,
            debug_ip=debug_ip,
            logger=logger
        )
        sessionizer.run()

    elif args.pipeline == 'train':
        trainer_parameters = {
            'warmup_period': int(os.environ.get('WARMUP_PERIOD')),
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
            'n_jobs': int(os.environ.get('N_JOBS'))
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
            'model_reload_in_minutes': int(os.environ.get('PREDICTOR_MODEL_RELOAD_IN_MINUTES')),
            'min_session_duration': int(os.environ.get('MIN_SESSION_DURATION')),
            'min_number_of_requests': int(os.environ.get('MIN_NUMBER_OF_REQUESTS')),
            'max_offences_before_blocking': int(os.environ.get('MAX_OFFENCES_BEFORE_BLOCKING')),
            'batch_size': int(os.environ.get('BATCH_SIZE')),
            's3_path': os.environ.get('S3_MODEL_STORAGE_PATH'),
            'whitelist_ip': os.environ.get('WHITELIST_IP'),
            'pending_ttl': int(os.environ.get('PENDING_TTL')),
            'maxsize_pending': int(os.environ.get('MAXSIZE_PENDING')),
            'datetime_format': os.environ.get('DATETIME_FORMAT'),
            'n_jobs_predict': int(os.environ.get('N_JOBS_PREDICT')),
            'whitelist_url': os.environ.get('WHITELIST_URL'),
            'bad_bot_challenge': os.environ.get('BAD_BOT_CHALLENGE') == 'True'
        }

        predictor = BaskervillehallPredictor(
            **predictor_parameters,
            kafka_connection=kafka_connection,
            s3_connection=s3_connection,
            debug_ip=debug_ip,
            logger=logger
        )
        predictor.run()
    else:
        logger.error('Pipeline is not specified. Use session, predict or train')

    logger.info('Pipeline finished.')
    exit()


if __name__ == '__main__':
    main()


