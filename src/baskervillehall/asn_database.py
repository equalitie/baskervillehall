import os
import csv
from typing import Union


class ASNDatabase:
    def __init__(self, csv_path: str):
        if not os.path.isfile(csv_path):
            raise FileNotFoundError(f"ASN file not found: {csv_path}")

        self.datacenter_asns = set()
        self.asn_entities = {}
        self._load_asns(csv_path)

    def _normalize_asn(self, asn: Union[str, int]) -> str:
        """
        Normalize ASN format by stripping 'AS' and converting to string.
        """
        return str(asn).strip().upper().lstrip("AS")

    def _load_asns(self, path: str) -> None:
        """
        Read the CSV and cache the ASNs and their entities.
        """
        with open(path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                asn = self._normalize_asn(row["ASN"])
                entity = row.get("Entity", "").strip()
                self.datacenter_asns.add(asn)
                self.asn_entities[asn] = entity

    def is_datacenter_asn(self, asn: Union[str, int]) -> bool:
        """
        Check if an ASN is in the known datacenter/VPN ASN list or contains datacenter-related keywords.
        """
        normalized_asn = self._normalize_asn(asn)
        if normalized_asn in self.datacenter_asns:
            return True
            
        normalized_asn = normalized_asn.lower()
        keywords = ['hosting', 'colo', 'cloud']
        return any(keyword in normalized_asn for keyword in keywords)

    def get_entity(self, asn: Union[str, int]) -> Union[str, None]:
        """
        Get the entity name associated with the ASN, if available.
        """
        return self.asn_entities.get(self._normalize_asn(asn))
