import requests
import threading
from datetime import datetime, timedelta

class TorExitScanner:
    def __init__(self, refresh_interval_hours=4):
        self.refresh_interval = timedelta(hours=refresh_interval_hours)
        self.tor_ips = set()
        self.last_refresh = None
        self._lock = threading.Lock()
        self.update_tor_list()  # Initial fetch

    def update_tor_list(self):
        try:
            response = requests.get("https://check.torproject.org/exit-addresses", timeout=10)
            response.raise_for_status()
            new_ips = {
                line.split()[1]
                for line in response.text.splitlines()
                if line.startswith("ExitAddress")
            }

            with self._lock:
                self.tor_ips = new_ips
                self.last_refresh = datetime.utcnow()
                print(f"[{datetime.utcnow()}] Tor exit list updated. {len(new_ips)} IPs loaded.")
        except Exception as e:
            print(f"[{datetime.utcnow()}] Failed to update Tor list: {e}")

    def refresh_if_needed(self):
        if self.last_refresh is None or datetime.utcnow() - self.last_refresh > self.refresh_interval:
            self.update_tor_list()

    def is_tor(self, ip):
        self.refresh_if_needed()
        with self._lock:
            return ip in self.tor_ips


# Example usage
if __name__ == "__main__":
    scanner = TorExitScanner(refresh_interval_hours=4)

    test_ip = "185.220.101.4"  # Example known Tor exit IP
    if scanner.is_tor(test_ip):
        print(f"{test_ip} is a Tor exit node.")
    else:
        print(f"{test_ip} is NOT a Tor exit node.")
