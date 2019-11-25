import unittest
import sys
import pandas as pd
from io import StringIO
sys.path.append("..")
from cleaners.rystadCleaner import clean, __setWeightedSulphur__, __arrangeColumns__, __getWeightedSulphur__

class TestRystadCleaner(unittest.TestCase):
    
    def setUp(self):
        self.testData = """
            2018|August|Not specified|United Kingdom|Crude Oil|Sweet|0.2|Production|10
            2018|August|Not specified|United Kingdom|Crude Oil|Slightly Sour|1.33|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.39|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.8|Production|10
        """
        self.df = pd.read_csv(StringIO(self.testData), header=None, sep='|',
                  names=['Year', 'Month', 'Crude Stream', 'Country', 'Oil and Gas Category', 'Sulphur Group', 'Sulphur Detail','[Data Values]', 'Sum'])
    
    # python -m unittest test_rystadCleaner.TestRystadCleaner.test__arrangeColumns__
    def test__arrangeColumns__(self):
        testData = """
            2018|August|Not specified|United Kingdom|Crude Oil|Sweet|0.2|Production|10
            2018|August|Not specified|United Kingdom|Crude Oil|Slightly Sour|1.33|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.39|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.8|Production|10
        """
        df = pd.read_csv(StringIO(self.testData), header=None, sep='|',
                  names=['Year', 'Month', 'Crude Stream', 'Country', 'Oil and Gas Category', 'Sulphur Group', 'Sulphur Detail','[Data Values]', 'Sum'])

        newCols = ['Now', 'Test', 'Different', 'Columns', 'against', 'the', 'old','values', '!']
        __arrangeColumns__(df)
        #self.assertEqual(self.ieaIntegrator.__configureDataFrame__(df, newCols).columns.tolist(), newCols)

    # python -m unittest test_rystadCleaner.TestRystadCleaner.test__getWeightedSulphur__
    def test__getWeightedSulphur__(self):
        testData = """
            2018|August|Not specified|United Kingdom|Crude Oil|Sweet|0.2|Production|10
            2018|August|Not specified|United Kingdom|Crude Oil|Slightly Sour|1.33|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.39|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.8|Production|10
        """
        df = pd.read_csv(StringIO(self.testData), header=None, sep='|',
                  names=['Year', 'Month', 'Crude Stream', 'Country', 'Oil and Gas Category', 'Sulphur Group', 'Sulphur Detail','[Data Values]', 'Sum'])

        newCols = ['Year','Month','Crude Stream','Country','Oil and Gas Category','Sulphur Group','Sulphur Detail','[Data Values]','Sum_x','ProductionSulphur_x','ProductionSulphur_y','Sum_y','WeightedSulphur']
        self.assertEqual(__getWeightedSulphur__(df).columns.tolist(), newCols)
    
    # python -m unittest test_rystadCleaner.TestRystadCleaner.test__setWeightedSulphur__
    def test__setWeightedSulphur__(self):
        self.assertEqual(__setWeightedSulphur__(self.df.iloc[0]), 0.2)