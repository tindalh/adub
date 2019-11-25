import unittest
import sys
sys.path.append("..")
from log import log

class TestLog(unittest.TestCase):

    # python -m unittest test_log.TestLog.test_log
    def test_log(self):
        self.assertIsInstance(log(__name__, 'test_log', "short function name"), str)
        self.assertIsInstance(log(__name__, 'really_long_function_name_to_test_alignment_log', "long function name"), str)