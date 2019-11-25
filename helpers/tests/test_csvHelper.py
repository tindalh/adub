import unittest
import pandas as pd
import sys
sys.path.append("..")
from csvHelper import getDataframe

class TestCsvHelper(unittest.TestCase):

    # python -m unittest test_csvHelper.TestCsvHelper.test_getDataFrame
    def test_getDataFrame(self):
        self.assertEqual(getDataframe("data\multi_space_separated.txt", '\s+').iloc[0].tolist(),['AUSTRALIA', 'CRUDE', 'FEB2005', 390.358])

        self.assertEqual(getDataframe("data\multi_space_separated.txt", '\s+', names=['a','b','c','d']).columns.tolist() ,['a','b','c','d'])

        self.assertEqual(getDataframe("data\comma_separated.csv", ',').iloc[0].tolist(),[2013,'January','Aasgard Blend'])

        self.assertEqual(getDataframe("data\\tab_separated.csv", '\t').iloc[0].tolist(),['01/01/2015','AGIOI THEODOROI', 'GREECE','EMED'])

if(__name__ == '__main__'):
    unittest.main()