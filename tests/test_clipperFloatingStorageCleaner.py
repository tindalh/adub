import unittest
import sys
sys.path.append("..")
import integrators.csvIntegrator as csvIntgrtr
from cleaners.clipperFloatingStorageCleaner import __arrangeClipperColumns__
import os
from io import StringIO
import pandas as pd

class TestClipperFloatingStorageCleaner(unittest.TestCase):
    # python -m unittest test_clipperFloatingStorageCleaner.TestClipperFloatingStorageCleaner.test__arrangeClipperColumns__
    def test__arrangeClipperColumns__(self):
        mystr = """
            01-01-2017
            01-01-2019
        """
        columns = ['date_asof']
        
        df = pd.read_csv(StringIO(mystr), header=None, sep='|', names=columns)

        self.assertEqual(__arrangeClipperColumns__(df, '2018-01-01').iloc[0].tolist(), ['2019-01-01'])


if(__name__ == '__main__'):
    unittest.main() 