import unittest
from baskervillehall.bot_verificator import BotVerificator


class TestBotVerificator(unittest.TestCase):

    def test_legit_crawlers(self):
        verificator = BotVerificator()

        self.assertTrue(not verificator.is_verified_bot(ip='1.1.1.1'))

        # google
        self.assertTrue(verificator.is_verified_bot(ip='66.249.66.1'))

        # bing
        self.assertTrue(verificator.is_verified_bot(ip='157.55.39.1'))

        # duckduckgo
        self.assertTrue(verificator.is_verified_bot(ip='4.182.131.108'))
