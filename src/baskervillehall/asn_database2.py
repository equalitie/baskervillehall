from asn_loader import ASNLoader
import os


class ASNDatabase2:
    def __init__(self, path):
        malicious_asn_path = os.path.join(path, 'Malicious', 'ASN.txt')
        vpn_asn_path = os.path.join(path, 'VPN Providers', 'ASN.txt')
        vps_asn_path = os.path.join(path, 'VPS Providers', 'ASN.txt')
        self.malicious_asn_loader = ASNLoader(malicious_asn_path)
        self.vpn_asn_loader = ASNLoader(vpn_asn_path)
        self.vps_asn_loader = ASNLoader(vps_asn_path)

    def is_malicious_asn(self, asn):
        return asn in self.malicious_asn_loader.asn_dict

    def is_vpn_asn(self, asn):
        return asn in self.vpn_asn_loader.asn_dict

    def is_tor_asn(self, asn):
        return asn in self.tor_asn_loader.asn_dict

    def is_vps_asn(self, asn):
        return asn in self.vps_asn_loader.asn_dict

    def get_asn_name(self, asn):
        for loader in [
            self.malicious_asn_loader,
            self.vpn_asn_loader,
            self.tor_asn_loader,
            self.vps_asn_loader,
        ]:
            name = loader.get_name(asn)
            if name:
                return name
        return None

    def get_all_malicious_asns(self):
        return self.malicious_asn_loader.all()

    def get_all_vpn_asns(self):
        return self.vpn_asn_loader.all()

    def get_all_tor_asns(self):
        return self.tor_asn_loader.all()

    def get_all_vps_asns(self):
        return self.vps_asn_loader.all()
