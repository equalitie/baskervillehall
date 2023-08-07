import logging
import threading
import time

from cachetools import TTLCache

from baskervillehall.model_io import ModelIO


class ModelStorage(object):

    def __init__(
            self,
            s3_connection,
            s3_path,
            reload_in_minutes=10,
            max_models=10000,
            logger=None
    ):
        super().__init__()
        self.s3_path = s3_path
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.models = TTLCache(maxsize=max_models, ttl=reload_in_minutes*60)
        self.model_io = ModelIO(**s3_connection, logger=self.logger)
        self.thread = None
        self.lock = threading.Lock()
        self.requests = set()

    def get_model(self, host):
        with self.lock:
            try:
                model = self.models[host]
            except KeyError as e:
                model = None

            if model:
                if host in self.requests:
                    self.requests.remove(host)
                return model

            self.requests.add(host)
            return None

    def _run(self):
        self.logger.info('Starting ModelStorage thread...')

        while True:
            with self.lock:
                host = self.requests.pop() if len(self.requests) else None
            if host is None:
                time.sleep(1)
                continue

            model = self.model_io.load(self.s3_path, host)
            if model is None:
                with self.lock:
                    self.requests.add(host)
                time.sleep(1)
                continue
            with self.lock:
                self.models[host] = model
                self.logger.info(f'Loaded model for host {host}. Total models = {len(self.models.keys())}')

    def start(self):
        if self.thread is not None:
            return

        self.thread = threading.Thread(target=self._run)
        self.thread.start()

