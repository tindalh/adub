import unittest
import sys
sys.path.append("..")
import integrators.csvIntegrator as csvIntgrtr

import importers.eiaImporter as eia
import os
from io import StringIO
import pandas as pd
from service_constants import *

class CSVIntegratorCase(unittest.TestCase):
    
    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaSupplyIntegrator
    def test_run_ieaSupplyIntegrator(self):
        ieaSupplyIntegrator.run(os.path.join(ieaSupplyIntegrator.file_path, 'Supply.txt'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaStockdatIntegrator
    def test_run_ieaStockdatIntegrator(self):
        ieaStockdatIntegrator.run(os.path.join(ieaStockdatIntegrator.file_path, 'Stockdat.txt'))
    
    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaSummaryIntegrator
    def test_run_ieaSummaryIntegrator(self):
        ieaSummaryIntegrator.run(os.path.join(ieaSummaryIntegrator.file_path, 'SUMMARY.txt'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaNOECDDEIntegrator
    def test_run_ieaNOECDDEIntegrator(self):
        ieaNOECDDEIntegrator.run(os.path.join(ieaNOECDDEIntegrator.file_path, 'NOECDDE.txt'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaOECDDEIntegrator
    def test_run_ieaOECDDEIntegrator(self):
        ieaOECDDEIntegrator.run(os.path.join(ieaOECDDEIntegrator.file_path, 'OECDDE.txt'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaCrudeDataIntegrator
    def test_run_ieaCrudeDataIntegrator(self):
        ieaCrudeDataIntegrator.run(os.path.join(ieaCrudeDataIntegrator.file_path, 'CRUDEDAT.txt'))
    
    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaExportDataIntegrator
    def test_run_ieaExportDataIntegrator(self):
        ieaExportDataIntegrator.run(os.path.join(ieaExportDataIntegrator.file_path, 'ExporDAT.txt'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaImportDataIntegrator
    def test_run_ieaImportDataIntegrator(self):
        ieaImportDataIntegrator.run(os.path.join(ieaImportDataIntegrator.file_path, 'ImporDAT.txt'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaProdDataIntegrator
    def test_run_ieaProdDataIntegrator(self):
        ieaProdDataIntegrator.run(os.path.join(ieaProdDataIntegrator.file_path, 'PRODDAT.txt'))



    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaFieldIntegrator
    def test_run_ieaFieldIntegrator(self):
        ieaFieldIntegrator.run(os.path.join(ieaFieldIntegrator.file_path, 'field_by_field.csv'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaCountryDetailsIntegrator
    def test_run_ieaCountryDetailsIntegrator(self):
        ieaCountryDetailsIntegrator.run(os.path.join(ieaCountryDetailsIntegrator.file_path, 'country_details.csv'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_ieaFieldDetailsIntegrator
    def test_run_ieaFieldDetailsIntegrator(self):
        ieaFieldDetailsIntegrator.run(os.path.join(ieaFieldDetailsIntegrator.file_path, 'field_details.csv'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_rystad
    def test_run_rystad(self):            
        rystadIntegrator.run(os.path.join(rystadIntegrator.file_path, 'Rystad Production_2013.csv'))
        # rystadIntegrator.run(os.path.join(rystadIntegrator.file_path, 'Rystad Production_2014.csv'))
        # rystadIntegrator.run(os.path.join(rystadIntegrator.file_path, 'Rystad Production_2015.csv'))
        # rystadIntegrator.run(os.path.join(rystadIntegrator.file_path, 'Rystad Production_2016.csv'))
        # rystadIntegrator.run(os.path.join(rystadIntegrator.file_path, 'Rystad Production_2017.csv'))
        # rystadIntegrator.run(os.path.join(rystadIntegrator.file_path, 'Rystad Production_2018.csv'))
        # rystadIntegrator.run(os.path.join(rystadIntegrator.file_path, 'Rystad Production_2019.csv'))

     # python -m unittest test_involved.CSVIntegratorCase.test_run_mcQuilling
    def test_run_mcQuilling(self):            
        mcQuillingIntegrator.run(os.path.join(mcQuillingIntegrator.file_path, 'McQuilling_20200123.csv'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_LSGasOil1930
    def test_run_LSGasOil1930(self):
        
        LSGasOil1930.run(os.path.join(LSGasOil1930.file_path, 'ICE 1930 LS Gas Oil Futures curve on 04-Feb-20_20200204.csv'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_BrentCrude1630
    def test_run_BrentCrude1630(self):
        
        BrentCrude1630.run(os.path.join(BrentCrude1630.file_path, 'ICE 1630 Brent Crude Futures curve on 15-Jan-20_20200115.csv'))

    # python -m unittest test_involved.CSVIntegratorCase.test_run_clipperFloatingStorage
    def test_run_clipperFloatingStorage(self):            
        clipperFloatingStorageIntegrator.run(os.path.join(clipperFloatingStorageIntegrator.file_path, 'ClipperData Global Crude Oil Floating Storage (7).csv'))
    
    # python -m unittest test_involved.CSVIntegratorCase.test__arrangeColumns__
    def test__arrangeColumns__(self):
        mystr = """
            2018|June|Matthew
            2017|August|Mark
            2014|September|Luke
            2016|December|John
        """
        df = pd.read_csv(StringIO(mystr), header=None, sep='|',
                  names=['Year', 'Month', 'Name'])
        
        table_columns = ['Name', 'Year', 'Month']
        dfResult = ieaSupplyIntegrator.__arrangeColumns__(df, table_columns)
        
        self.assertEqual(dfResult.columns.tolist(), table_columns)
