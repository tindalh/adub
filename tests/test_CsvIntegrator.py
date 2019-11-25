import unittest
import sys
sys.path.append("..")
import integrators.csvIntegrator as csvIntgrtr
from cleaners.ieaCleaner import clean as cleanIea
from cleaners.rystadCleaner import clean as rystadCleaner
from cleaners.clipperFloatingStorageCleaner import clean as clipperFloatingStorageCleaner
import os
from io import StringIO
import pandas as pd

class TestCsvIntegrator(unittest.TestCase):
    def setUp(self):
        self.ieaIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Integrator',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIea,
            delimiter='\s+'
        )

        self.rystadIntegrator = csvIntgrtr.CsvIntegrator(
            name='Rystad Integrator',
            server=os.environ['ADUB_DBServer'],
            database='Analytics',
            table_name='yview_RystadProduction',
            file_path="{}\\RystadProduction".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\RystadProduction\\".format(os.environ['ADUB_Import_Output_UNC']), 
            truncate=False,
            column_for_delete='Period',
            clean = rystadCleaner,
            delimiter=','
        )

        self.clipperFloatingStorageIntegrator = csvIntgrtr.CsvIntegrator(
            name='Clipper Floating Storage Integrator',
            server=os.environ['ADUB_DBServer'],
            database='STG_Targo',
            table_name='yview_ClipperFloatingStorage',
            file_path="{}\\ClipperFloatingStorage".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\ClipperFloatingStorage\\".format(os.environ['ADUB_Import_Output_UNC']), 
            truncate=False,
            column_for_delete='date_asof',
            clean = clipperFloatingStorageCleaner,
            clean_arg='2015-01-01',
            delimiter=','
        )
    
    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run
    def test_run(self):
        self.ieaIntegrator.run(os.path.join(self.ieaIntegrator.file_path, 'SUPPLY.txt'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_rystad
    def test_run_rystad(self):            
        self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2013.csv'))
        self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2014.csv'))
        self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2015.csv'))
        self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2016.csv'))
        self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2017.csv'))
        self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2018.csv'))
        self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2019.csv'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_clipperFloatingStorage
    def test_run_clipperFloatingStorage(self):            
        self.clipperFloatingStorageIntegrator.run(os.path.join(self.clipperFloatingStorageIntegrator.file_path, 'ClipperData Global Crude Oil Floating Storage (3).csv'))
    
    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_getImportFileNames
    def test_getImportFileNames(self):
        self.assertIsNotNone(self.ieaIntegrator.__getImportFileNames__())

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test__configureDataFrame__
    def test__configureDataFrame__(self):
        mystr = """
            2018|August|Not specified|United Kingdom|Crude Oil|Sweet|0.2|Production|10
            2018|August|Not specified|United Kingdom|Crude Oil|Slightly Sour|1.33|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.39|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.4|Production|10
        """
        df = pd.read_csv(StringIO(mystr), header=None, sep='|',
                  names=['Year', 'Month', 'Crude Stream', 'Country', 'Oil and Gas Category', 'Sulphur Group', 'Sulphur Detail','[Data Values]', 'Sum'])

        newCols = ['Now', 'Test', 'Different', 'Columns', 'against', 'the', 'old','values', '!']
        self.assertEqual(self.ieaIntegrator.__configureDataFrame__(df, newCols).columns.tolist(), newCols)

if(__name__ == '__main__'):
    unittest.main() 