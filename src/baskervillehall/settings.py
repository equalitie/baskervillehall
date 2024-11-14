import time


class Settings(object):

    def __init__(self,
                 refresh_period_in_seconds=180
                 ):
        self.refresh_period_in_seconds = refresh_period_in_seconds
        self.last_timestamp = None
        self.settings = dict()
        super().__init__()

    def read(self):
        pass

    def get_sensitivity(self, host):
        self.refresh()
        return self.settings.get(host, {}).get('sensitivity', 0)

    def refresh(self):
        if not self.last_timestamp or int(time.time() - self.last_timestamp) > self.refresh_period_in_seconds:
            self.last_timestamp = time.time()
            self.read()

