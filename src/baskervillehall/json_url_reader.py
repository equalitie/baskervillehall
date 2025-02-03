from urllib.error import HTTPError, URLError
from urllib.request import urlopen
import json
import time


class JsonUrlReader(object):
    def __init__(self, url, logger=None, refresh_period_in_seconds=300):
        self.url = url
        self.refresh_period_in_seconds = refresh_period_in_seconds
        self.last_timestamp = None
        self.logger = logger
        self.data = None

    def read_json_from_url(self, url):
        try:
            html = urlopen(url, timeout=3).read()
        except TimeoutError as e:
            if self.logger:
                self.logger.error(f'Timeout while parsing url {url}', e)
            return None
        except HTTPError as e:
            if self.logger:
                self.logger.error(f'HTTP Error {e.code} while parsing url {url}')
            return None
        except URLError as e:
            if self.logger:
                self.logger.error(f'URL error {e.reason} while getting from {url}')
            return None
        except Exception as e:
            if self.logger:
                self.logger.error(e)

        try:
            data = json.loads(html)
        except json.JSONDecodeError as e:
            if self.logger:
                self.logger.error(f'JSON error {e} while getting from {url}')
            return None

        return data

    def refresh(self):
        if not self.url:
            return False
        if not self.last_timestamp or int(time.time() - self.last_timestamp) > self.refresh_period_in_seconds:
            self.last_timestamp = time.time()
            if self.url:
                if self.logger:
                    self.logger.info(f'Refreshing from url...')
                self.data = self.read_json_from_url(self.url)
                return True
        return False

    def get(self):
        fresh = self.refresh()
        return self.data, fresh
