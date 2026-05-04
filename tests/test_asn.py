import unittest
from baskervillehall.asn_database import ASNDatabase

class TestASNDatabase(unittest.TestCase):
    def test_datacenter(self):
        db = ASNDatabase("../deployment/bad_asn/bad-asn-list.csv")
        self.assertTrue(db.is_datacenter_asn("AS16509"))  # True (Amazon)
        self.assertTrue(db.is_datacenter_asn(8075))       # True (Azure)
        self.assertFalse(db.is_datacenter_asn("12345"))    # False



