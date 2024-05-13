import logging
import threading
import time
from datetime import datetime

from baskervillehall.model_io import ModelIO


class ModelStorage(object):

    def __init__(
            self,
            s3_connection,
            s3_path,
            human,
            reload_in_minutes=10,
            logger=None
    ):
        super().__init__()
        self.s3_path = s3_path
        self.logger = logger if logger else logging.getLogger(self.__class__.__name__)
        self.models = {}
        self.model_io = ModelIO(**s3_connection, logger=self.logger)
        self.thread = None
        self.reload_in_minutes = reload_in_minutes
        self.lock = threading.Lock()
        self.human = human
        self.requests = set()

    def _is_expired(self, ts):
        return (datetime.now() - ts).total_seconds() > self.reload_in_minutes * 60

    def get_model(self, host):
        with self.lock:
            model, ts = self.models.get(host, (None, None))
            if model and ts:
                if self._is_expired(ts):
                    self.requests.add(host)
                elif host in self.requests:
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

            model = self.model_io.load(self.s3_path, host, human=self.human)
            if model is None:
                with self.lock:
                    self.requests.add(host)
                continue
            with self.lock:
                self.models[host] = (model, datetime.now())
                self.logger.info(f'Loaded model for host {host} {"human" if self.human else "bot"}). '
                                 f'Total models = {len(self.models.keys())}')

    def start(self):
        if self.thread is not None:
            return

        self.thread = threading.Thread(target=self._run)
        self.thread.start()

