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

     # python -m unittest test_rystadCleaner.TestRystadCleaner.test_clean
    def test_clean(self):   
        dataIn = """
            2019|January|Alabama Sweet|United States|Crude Oil|Sweet|0.4|Production|8.956806
            2019|January|Alabama Sweet|United States|Condensate|Sweet|0.4|Production|5.154038
        """
        dfIn = pd.read_csv(StringIO(dataIn), header=None, sep='|',
                  names=['Year', 'Month', 'Crude Stream', 'Country', 'Oil and Gas Category', 'Sulphur Group', 'Sulphur Detail','[Data Values]', 'Sum'])

        dataOut = """United States|Crude Oil|Alabama Sweet|2019-01-01|8.956806|Sweet|0.4|0.4\r\nUnited States|Condensate|Alabama Sweet|2019-01-01|5.154038|Sweet|0.4|0.4
        """
        dfOut = pd.read_csv(StringIO(dataOut), header=None, sep='|',
                  names=[ 'Country', 'Oil and Gas Category', 'Crude Stream', 'Period','Sum_x', 'Sulphur Group', 'Sulphur Detail','WeightedSulphur'])

        dfResult = clean(dfIn)

        #print (f"Expected\n {dfOut}")
        #print (f"Actual\n {dfResult}")
        self.assertEqual(dfResult.columns.tolist(),dfOut.columns.tolist())
        self.assertEqual(dfResult['Sum_x'].iloc[0], 8.956806)