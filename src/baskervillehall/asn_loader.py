import re

class ASNLoader:
    def __init__(self, path):
        self.asn_dict = {}         # AS number (int) → name
        self.name_to_asn = {}      # Lowercase name → AS number(s)
        self._load(path)

    def _load(self, path):
        with open(path, 'r', encoding='utf-8') as file:
            for line in file:
                line = line.strip()
                if not line or not line.startswith("AS"):
                    continue

                match = re.match(r'^AS(\d+)\s+(.+)$', line)
                if not match:
                    continue

                asn = int(match.group(1))
                name = match.group(2).strip()

                self.asn_dict[asn] = name
                self.name_to_asn.setdefault(name.lower(), []).append(asn)

    def get_name(self, asn):
        return self.asn_dict.get(asn)

    def get_asns_by_name(self, name_substring):
        """Fuzzy search by substring (case-insensitive)"""
        name_substring = name_substring.lower()
        results = {}
        for name, asns in self.name_to_asn.items():
            if name_substring in name:
                for asn in asns:
                    results[asn] = self.asn_dict[asn]
        return results

    def all(self):
        return self.asn_dict
