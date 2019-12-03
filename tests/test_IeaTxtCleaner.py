import unittest
import sys
import pandas as pd
sys.path.append("..")
from cleaners.ieaTxtCleaner import __replaceInvalidQuantities__
from cleaners.ieaTxtCleaner import __cleanDateColumns__
from cleaners.ieaTxtCleaner import __setFrequency__
from cleaners.ieaTxtCleaner import clean
import datetime

class TestIeaTxtCleaner(unittest.TestCase):
    # python -m unittest test_IeaTxtCleaner.TestIeaTxtCleaner.test_replaceInvalidQuantities
    def test_replaceInvalidQuantities(self):
        d = {'colA':[1,2,3], 'Quantity':['x', 200,300],'colC':['x','y','z']}
        df = pd.DataFrame(data=d)

        self.assertEqual(__replaceInvalidQuantities__(df).iloc[0]['Quantity'], '0')

    # python -m unittest test_IeaTxtCleaner.TestIeaTxtCleaner.test__cleanDateColumns__
    def test__cleanDateColumns__(self):
        frequency_indicators = ['Month', 'Quarter','Year']

        dIn = {
            'Period':pd.Series(['JAN2019', '2Q2005','MAR-2018'],dtype='str'),
            'FREQUENCY':pd.Series(['Monthly','YEARLY','Quarterly'],dtype='str')
        }
        dfIn = pd.DataFrame(data=dIn)

        dOut = {
            'Period':pd.Series(['2019-01-01', '2005-04-01','2018-03-01'],dtype='datetime64[ns]'),
            'FREQUENCY':pd.Series(['Monthly','YEARLY','Quarterly'],dtype='str')
        }
        dfOut = pd.DataFrame(data=dOut)
        dfTest = __cleanDateColumns__(dfIn)
        self.assertTrue(dfTest.equals(dfOut))   

    # python -m unittest test_IeaTxtCleaner.TestIeaTxtCleaner.test__setFrequency__
    def test__setFrequency__(self):
        dIn = {
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'Period':pd.Series(['1Q2015','3Q2014','2018','JAN2015','DEC2015'], dtype='str')
        }
        dfIn = pd.DataFrame(data=dIn)

        dOut = {
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'Period':pd.Series(['1Q2015','3Q2014','2018','JAN2015','DEC2015'], dtype='str'),
            'PeriodType':pd.Series(['Quarter','Quarter', 'Year', 'Month','Month'], dtype='str')
        }
        
        dfOut = pd.DataFrame(data=dOut)
        dfTest = __setFrequency__(dfIn)
        
        self.assertTrue(dfTest.equals(dfOut))

    # python -m unittest test_IeaTxtCleaner.TestIeaTxtCleaner.test__setFrequency__for_new_csv_format
    def test__setFrequency__for_new_csv_format(self):
        dIn = {
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'FREQUENCY':pd.Series(['monthly','quarterly','monthly','yearly','quarterly'], dtype='str')
        }
        dfIn = pd.DataFrame(data=dIn)

        dOut = {
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'FREQUENCY':pd.Series(['monthly','quarterly','monthly','yearly','quarterly'], dtype='str'),
            'PeriodType':pd.Series(['Month','Quarter', 'Month', 'Year','Quarter'], dtype='str')
        }
        
        dfOut = pd.DataFrame(data=dOut)
        dfTest = __setFrequency__(dfIn)
        
        self.assertTrue(dfTest.equals(dfOut))

    # python -m unittest test_IeaTxtCleaner.TestIeaTxtCleaner.test_clean
    def test_clean(self):
        dIn = {
            'Quantity':pd.Series([12,25,36,'x',55]),
            'Period':pd.Series(['1Q2015','3Q2014','2018','JAN2015','DEC2015'], dtype='str'),
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'Product':pd.Series(['CRUDE','NG','LNG','Stuff','Coal'], dtype='str')
        }
        dfIn = pd.DataFrame(data=dIn)

        dOut = {
            'Quantity':pd.Series([12,25,36,'0',55]),
            'Period':pd.Series(['2015-01-01','2014-07-01','2018-01-01','2015-01-01','2015-12-01'], dtype='datetime64[ns]'),
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'Product':pd.Series(['CRUDE','NG','LNG','Stuff','Coal'], dtype='str'),
            'PeriodType':pd.Series(['Quarter','Quarter', 'Year', 'Month','Month'], dtype='str'),
            'Asof':pd.Series([datetime.date.today(),datetime.date.today(), datetime.date.today(), datetime.date.today(),datetime.date.today()])
        }
        dfOut = pd.DataFrame(data=dOut)
        dfTest = clean(dfIn)
        self.assertTrue(dfTest.equals(dfOut))

        # now test for the new format from
        # IEA

        dIn_newFormat = {
            # 'FIELD':pd.Series(['field one','field two']),
            # 'COUNTRY':pd.Series(['country one','country two']),
            # 'PRODUCT':pd.Series(['product one','product two']),
            # 'ENVIRONMENT':pd.Series(['environment one','environment two']),
            'TIME':pd.Series(['01/09/1994','01/09/1994','01/09/1994'], dtype='str'),
            'FREQUENCY':pd.Series(['monthly','yearly','quarterly'], dtype='str'),
            'TIMESTAMP':pd.Series(['01/09/1994','01/09/1995','01/09/1996'], dtype='str'),
            'VALUE':pd.Series([1,2,3], dtype='str'),
        }
        dfIn_newFormat = pd.DataFrame(data=dIn_newFormat)

        dOut_newFormat = {
            'TIME':pd.Series(['01/09/1994','01/09/1994','01/09/1994'], dtype='str'),
            'FREQUENCY':pd.Series(['monthly','yearly','quarterly'], dtype='str'),
            'TIMESTAMP':pd.Series(['01/09/1994','01/09/1995','01/09/1996'], dtype='str'),
            'VALUE':pd.Series([1,2,3], dtype='str'),            
            'PeriodType':pd.Series(['Month','Year', 'Quarter'], dtype='str'),            
            'Asof':pd.Series([datetime.date.today(),datetime.date.today(), datetime.date.today()]),
            'Period':pd.Series(['1994-01-09','1995-01-09','1996-01-09'], dtype='datetime64[ns]')
        }
        dfOut_newFormat = pd.DataFrame(data=dOut_newFormat)
        dfTest_newFormat = clean(dfIn_newFormat)

        self.assertTrue(dfTest_newFormat.equals(dfOut_newFormat))