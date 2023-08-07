import logging

import boto3
import os
import pickle
from datetime import datetime

from botocore.exceptions import ClientError


class ModelIO(object):

    def __init__(self, s3_endpoint, s3_access, s3_secret, s3_region, logger=None):
        self.s3_endpoint = s3_endpoint
        self.s3_access = s3_access
        self.s3_secret = s3_secret
        self.s3_region = s3_region
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _get_timestamp():
        """
        Get a string UTC timestamp
        :return: YYYY_MM_DD__HH:MM
        """
        ts = datetime.utcnow()
        return f'{ts.year:02}_{ts.month:02}_{ts.day:02}___{ts.hour:02}_{ts.minute:02}'

    @staticmethod
    def _split_s3_path(s3_path):
        path_parts = s3_path.replace("s3://", "").split("/")
        bucket = path_parts.pop(0)
        key = "/".join(path_parts)
        return bucket, key

    def _create_session(self):
        session = boto3.session.Session()
        return session.client(
            service_name='s3',
            aws_access_key_id=self.s3_access,
            aws_secret_access_key=self.s3_secret,
            endpoint_url='https://' + self.s3_endpoint,
            region_name=self.s3_region
        )

    def _load_object(self, path):
        s3 = self._create_session()
        bucket, key = self._split_s3_path(path)
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read()

    def _save_object(self, body, path):
        s3 = self._create_session()
        bucket, key = self._split_s3_path(path)
        s3.put_object(Body=body, Bucket=bucket, Key=key)

    def save(self, model, path, host):
        model_folder = os.path.join(path, host)
        model_path = os.path.join(model_folder, f'{self._get_timestamp()}.pkl')

        self._save_object(pickle.dumps(model), model_path)
        self._save_object(model_path, os.path.join(model_folder, 'index.txt'))

        self.logger.info(f'Model for host {host} saved into {model_path}')

    def load(self, path, host):
        try:
            model_folder = os.path.join(path, host)
            model_path = self._load_object(os.path.join(model_folder, 'index.txt')).decode('utf-8')

            model_data = self._load_object(model_path)
            model = pickle.loads(model_data)
            return model

        except ClientError as ex:
            if ex.response['Error']['Code'] != 'NoSuchKey':
                self.logger.info(ex)
            return None

    def load_exact_path(self, path):
        try:
            return pickle.loads(self._load_object(path))

        except ClientError as ex:
            if ex.response['Error']['Code'] != 'NoSuchKey':
                self.logger.info(ex)
            return None
