import argparse
import logging
import multiprocessing as mp

import os

import json

from baskervillehall.baskervillehall_session import BaskervillehallSession
from baskervillehall.storage_commands import StorageCommands
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
    Baskervillehall session pipeline only
    '''
    logger = get_logger(
        __name__,
        logging_level=logging.INFO
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        'pipeline',
        help='Pipeline to use: session only',
    )
    args = parser.parse_args()

    kafka_connection = {
        'bootstrap_servers': os.environ.get('BOOTSTRAP_SERVERS')
    }

    kafka_connection_commands = {
        'bootstrap_servers': os.environ.get('BOOTSTRAP_SERVERS_OUTPUT')
    }

    debug_ip = os.environ.get('DEBUG_IP')

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
            'group_id': os.environ.get('GROUP_ID_SESSION','session_pipeline)'),
            'min_session_duration': int(os.environ.get('MIN_SESSION_DURATION')),
            'max_session_duration': int(os.environ.get('MAX_SESSION_DURATION')),
            'primary_session_expiration': int(os.environ.get('PRIMARY_SESSION_EXPIRATION', 10)),
            'max_primary_sessions_per_ip': int(os.environ.get('MAX_PRIMARY_SESSIONS_PER_IP', 10)),
            'datetime_format': os.environ.get('DATETIME_FORMAT', "%Y-%m-%d %H:%M:%S"),
            'min_number_of_requests': int(os.environ.get('MIN_NUMBER_OF_REQUESTS', 5)),
            'deflect_config_url': os.environ.get('DEFLECT_CONFIG_URL'),
            'deflect_config_auth': os.environ.get('DEFLECT_CONFIG_AUTH'),
            'whitelist_url_default': os.environ.get('WHITELIST_URL_DEFAULT').split(','),
            'postgres_connection': postgres_connection,
            'lag_high_threshold': int(os.environ.get('LAG_HIGH_THRESHOLD', 3000)),
            'lag_moderate_threshold': int(os.environ.get('LAG_MODERATE_THRESHOLD', 1500)),
            'lag_emergency_threshold': int(os.environ.get('LAG_EMERGENCY_THRESHOLD', 8000)),
            'lag_critical_threshold': int(os.environ.get('LAG_CRITICAL_THRESHOLD', 6000)),
            'score_2_num_requests': int(os.environ.get('SCORE_2_NUM_REQUESTS', 5)),
            'topic_commands': os.environ.get('TOPIC_COMMANDS'),
            'dnet_partition_map': json.loads(os.environ.get('DNET_PARTITION_MAP', '{}')),
        }

        logger.info("Starting session pipeline")
        sessionizer = BaskervillehallSession(
            **params,
            kafka_connection=kafka_connection,
            kafka_connection_commands=kafka_connection_commands,
            debug_ip=debug_ip,
            asn_database_path=os.environ.get('BAD_ASN_FILE', ''),  # Optional - will be empty
            asn_database2_path=os.environ.get('VPN_ASN_PATH', ''),  # Optional - will be empty
            hostname=os.environ.get('MY_POD_NAME', 'SESSION_POD'),
            logger=logger
        )

        sessionizer.run()

        logger.info('Session pipeline finished.')
    elif args.pipeline == 'storage':
        params = {
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
            group_id=os.environ.get('GROUP_ID_STORAGE'),
            logger=logger
        )
        t1 = storage_sessions.start()

        storage_commands = StorageCommands(
            **params,
            topic=os.environ.get('TOPIC_COMMANDS'),
            kafka_connection=kafka_connection,
            num_requests=num_requests,
            table=os.environ.get('SQL_TABLE_COMMANDS'),
            group_id=os.environ.get('GROUP_ID_STORAGE'),
            logger=logger
        )
        t2 = storage_commands.start()

        # if partition == 0:
        #     alert = AlertChallengeRate(
        #         postgres_connection=postgres_connection,
        #         aggregation_window_in_minutes=int(os.environ.get('ALERT_AGGREGATION_WINDOW_IN_MINUTES')),
        #         dataset_in_hours=int(os.environ.get('ALERT_DATASET_IN_HOURS')),
        #         zscore_hits=float(os.environ.get('ALERT_ZSCORE_HITS')),
        #         zscore_challenge_rate=float(os.environ.get('ALERT_ZSCORE_CHALLENGE_RATE')),
        #         pending_period_in_minutes=int(os.environ.get('ALERT_PENDING_PERIOD_IN_MINUTES')),
        #         min_num_sessions=int(os.environ.get('ALERT_MIN_NUM_SESSIONS')),
        #         host_white_list=os.environ.get('ALERT_HOST_WHITE_LIST').split(','),
        #         webhook=os.environ.get('ALERT_WEBHOOK'),
        #         cc=os.environ.get('ALERT_CC'),
        #         threshold_challenge_rate=int(os.environ.get('ALERT_THRESHOLD_CHALLENGE_RATE')),
        #         logger=logger
        #     )
        #     t3 = alert.start()
        #     t3.join()

        t1.join()
        t2.join()
    else:
        logger.error(f'Pipeline "{args.pipeline}" is not supported.')

    exit()


if __name__ == '__main__':
    main()