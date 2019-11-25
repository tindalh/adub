import unittest
import sys
import pandas as pd
sys.path.append("..")
from cleaners.ieaCleaner import __replaceInvalidQuantities__
from cleaners.ieaCleaner import clean

class TestIeaCleaner(unittest.TestCase):
    # python -m unittest test_IeaCleaner.TestIeaCleaner.test_replaceInvalidQuantities
    def test_replaceInvalidQuantities(self):
        d = {'colA':[1,2,3], 'Quantity':['x', 200,300],'colC':['x','y','z']}
        df = pd.DataFrame(data=d)

        self.assertEqual(__replaceInvalidQuantities__(df).iloc[0]['Quantity'], '0')

    