import unittest
from baskervillehall.bot_verificator import BotVerificator


class TestBotVerificator(unittest.TestCase):

    def test_google(self):
        verificator = BotVerificator()
        self.assertTrue(not verificator.is_verified_bot(ip='1.1.1.1'))
        self.assertTrue(verificator.is_verified_bot(ip='66.249.66.1'))
