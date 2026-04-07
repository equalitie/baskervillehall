import logging
import time
import boto3

from botocore.config import Config
from botocore.exceptions import ClientError, ResponseStreamingError
import os
import pickle
from datetime import datetime


class ModelIO(object):

    def __init__(self, s3_endpoint, s3_access, s3_secret, s3_region, logger=None):
        self.s3_endpoint = s3_endpoint
        self.s3_access = s3_access
        self.s3_secret = s3_secret
        self.s3_region = s3_region
        self.logger = logger or logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _get_timestamp():
        ts = datetime.utcnow()
        return f'{ts.year:02}_{ts.month:02}_{ts.day:02}___{ts.hour:02}_{ts.minute:02}'

    @staticmethod
    def _split_s3_path(s3_path):
        parts = s3_path.replace("s3://", "").split("/")
        bucket = parts.pop(0)
        key = "/".join(parts)
        return bucket, key

    def _create_session(self):
        cfg = Config(
            signature_version='s3v4',
            s3={
                'addressing_style': 'path',      # https://endpoint/bucket/key
                'use_chunked_encoding': False    # send Content-Length instead of chunked
            },
            request_checksum_calculation='when_required',
            response_checksum_validation='when_required',
        )
        return boto3.session.Session().client(
            service_name='s3',
            aws_access_key_id=self.s3_access,
            aws_secret_access_key=self.s3_secret,
            endpoint_url=f'https://{self.s3_endpoint}',
            region_name=self.s3_region,
            config=cfg
        )

    def _load_object(self, path, max_retries=3):
        bucket, key = self._split_s3_path(path)
        for attempt in range(max_retries):
            s3 = self._create_session()
            try:
                return s3.get_object(Bucket=bucket, Key=key)['Body'].read()
            except (ResponseStreamingError, ConnectionError, OSError) as e:
                if attempt < max_retries - 1:
                    wait = 2 ** attempt
                    self.logger.warning(
                        f'S3 read failed for {key} (attempt {attempt + 1}/{max_retries}), '
                        f'retrying in {wait}s: {e}'
                    )
                    time.sleep(wait)
                else:
                    raise

    def _save_object(self, body, path):
        # Ensure body is bytes
        if isinstance(body, str):
            body = body.encode('utf-8')

        s3 = self._create_session()
        bucket, key = self._split_s3_path(path)
        try:
            s3.put_object(
                Body=body,
                Bucket=bucket,
                Key=key,
            )
        except ClientError as e:
            self.logger.error("S3 PutObject failed", exc_info=True)
            meta = e.response.get('ResponseMetadata', {})
            self.logger.error("ResponseMetadata: %r", meta)
            raise

    def save(self, model, path, host, model_type):
        folder = os.path.join(path, host, model_type.value)
        ts_path = os.path.join(folder, f'{self._get_timestamp()}.pkl')

        # upload the pickled model (with its checksum)
        self._save_object(pickle.dumps(model), ts_path)

        # update index file to point at the new timestamped .pkl
        # ts_path is a str, so will be encoded to UTF-8 inside _save_object
        self._save_object(ts_path, os.path.join(folder, 'index.txt'))

        self.logger.info(f'Model for host {host} saved into {ts_path}')

    def load(self, path, host, model_type):
        try:
            folder = os.path.join(path, host, model_type.value)
            idx = self._load_object(os.path.join(folder, 'index.txt')).decode('utf-8')
            data = self._load_object(idx)
            return pickle.loads(data)
        except ClientError as ex:
            if ex.response['Error']['Code'] != 'NoSuchKey':
                self.logger.info(ex)
            return None
        except (ResponseStreamingError, ConnectionError, OSError) as ex:
            self.logger.warning(f'Failed to load model for {host}/{model_type.value}: {ex}')
            return None

    def load_exact_path(self, path):
        try:
            return pickle.loads(self._load_object(path))
        except ClientError as ex:
            if ex.response['Error']['Code'] != 'NoSuchKey':
                self.logger.info(ex)
            return None
        except (ResponseStreamingError, ConnectionError, OSError) as ex:
            self.logger.warning(f'Failed to load model from {path}: {ex}')
            return None
