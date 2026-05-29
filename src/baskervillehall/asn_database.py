import os
import csv
from typing import Union


class ASNDatabase:
    def __init__(self, csv_path: str):
        self.datacenter_asns = set()
        self.asn_entities = {}
        
        # Handle empty or missing ASN database path
        if not csv_path or not csv_path.strip():
            # Initialize empty database when no path provided
            return
            
        if not os.path.isfile(csv_path):
            # Log warning but don't fail - initialize empty database
            return

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

    # Keywords that strongly indicate a datacenter/hosting ASN when found in entity name.
    # Intentionally excludes generic terms ('network', 'telecom', 'internet') that
    # also appear in residential ISP names and would cause false positives.
    DATACENTER_KEYWORDS = [
        'hosting', 'hoster',
        'colo', 'colocation',
        'datacenter', 'data center', 'data centre',
        'cloud', 'cloudhosting',
        'vps', 'virtual private server',
        'dedicated server', 'dedicated hosting',
        'server farm',
        'hetzner', 'ovh', 'digitalocean', 'linode', 'vultr',
        'amazon', 'google cloud', 'microsoft azure', 'alibaba cloud',
        'leaseweb', 'serverius', 'psychz', 'tzulo', 'nocser',
    ]

    def is_datacenter_asn(self, asn: Union[str, int], asn_name: str = '') -> bool:
        """
        Check if an ASN is a datacenter/hosting ASN by:
        1. CSV list lookup (bad-asn-list.csv)
        2. Keyword match against asn_name from GeoLite2 (primary — covers ASNs not in CSV)
        3. Keyword match against entity from CSV (fallback)
        """
        normalized_asn = self._normalize_asn(asn)
        if normalized_asn in self.datacenter_asns:
            return True

        # Check the live ASN name from GeoLite2 (most accurate, covers unlisted ASNs)
        if asn_name:
            name_lower = asn_name.lower()
            if any(keyword in name_lower for keyword in self.DATACENTER_KEYWORDS):
                return True

        # Fallback: check entity name from CSV for ASNs that have it
        entity = self.asn_entities.get(normalized_asn, '').lower()
        if entity:
            return any(keyword in entity for keyword in self.DATACENTER_KEYWORDS)

        return False

    def get_entity(self, asn: Union[str, int]) -> Union[str, None]:
        """
        Get the entity name associated with the ASN, if available.
        """
        return self.asn_entities.get(self._normalize_asn(asn))
