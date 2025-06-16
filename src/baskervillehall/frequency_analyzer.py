from collections import defaultdict, deque
from datetime import datetime, timedelta
import statistics
from math import log2

class FrequencyAnalyzer:
    def __init__(self, bucket_interval=60, max_age=600):
        """
        :param bucket_interval: seconds per bucket (e.g., 60 for 1 minute)
        :param max_age: total retention time in seconds (e.g., 600 for 10 minutes)
        """
        self.bucket_interval = bucket_interval
        self.max_age = max_age
        self.buckets = deque()  # each item: (bucket_start_time, latest_ts, {}, {host: {key: count}})
        self._zscore_cache = {}  # {host: {"bucket_ts": timestamp, "z_scores": {key: z}}}

    def _get_bucket_start(self, timestamp: datetime) -> datetime:
        seconds = int(timestamp.timestamp())
        aligned = seconds - (seconds % self.bucket_interval)
        return datetime.fromtimestamp(aligned)

    def process(self, host: str, key: str, timestamp: datetime):
        """
        Add a single key hit for the given host and timestamp.
        """
        bucket_time = self._get_bucket_start(timestamp)

        if not self.buckets or self.buckets[-1][0] != bucket_time:
            self.buckets.append((bucket_time, timestamp, {}, defaultdict(lambda: defaultdict(int))))

        # Purge old buckets
        cutoff = timestamp - timedelta(seconds=self.max_age)
        while self.buckets and self.buckets[0][1] < cutoff:
            self.buckets.popleft()

        # Increment key count
        _, _, _, key_counts = self.buckets[-1]
        key_counts[host][key] += 1

    def get_entropy(self, host: str) -> float:
        """
        Returns normalized entropy [0.0–1.0] for key distribution under a host.
        """
        total = 0
        counts = defaultdict(int)

        for _, _, _, kc in self.buckets:
            for k, v in kc[host].items():
                counts[k] += v
                total += v

        if total == 0 or len(counts) <= 1:
            return 1.0  # Max uncertainty or insufficient data

        entropy = -sum((c / total) * log2(c / total) for c in counts.values())
        max_entropy = log2(len(counts))
        return entropy / max_entropy if max_entropy > 0 else 1.0

    def get_key_zscore(self, host: str, key: str, clip: bool = True) -> float:
        """
        Returns z-score for how frequently a key appears for the host.
        Uses a cache keyed on latest bucket timestamp to avoid recomputation.
        """
        if not self.buckets:
            return 0.0

        latest_ts = self.buckets[-1][0]
        cached = self._zscore_cache.get(host)

        if cached and cached["bucket_ts"] == latest_ts:
            return cached["z_scores"].get(key, 0.0)

        # Recalculate z-scores
        key_totals = defaultdict(int)
        for _, _, _, kc in self.buckets:
            for k, count in kc[host].items():
                key_totals[k] += count

        z_scores = {}
        if len(key_totals) >= 2:
            counts = list(key_totals.values())
            mean = statistics.mean(counts)
            stdev = statistics.stdev(counts)
            for k, count in key_totals.items():
                z = (count - mean) / stdev if stdev > 0 else 0.0
                z_scores[k] = max(0.0, z) if clip else z

        self._zscore_cache[host] = {
            "bucket_ts": latest_ts,
            "z_scores": z_scores
        }

        return z_scores.get(key, 0.0)

    def get_suspicious_keys(self, host: str, z_threshold: float = 3.0) -> list:
        """
        Returns a list of keys that have z-scores above the specified threshold.
        """
        if not self.buckets:
            return []

        latest_ts = self.buckets[-1][0]
        cached = self._zscore_cache.get(host)

        if not cached or cached["bucket_ts"] != latest_ts:
            self.get_key_zscore(host, "__dummy__", clip=True)  # Force cache update

        z_scores = self._zscore_cache[host]["z_scores"]
        return [k for k, z in z_scores.items() if z >= z_threshold]
