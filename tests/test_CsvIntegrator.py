import unittest
import sys
sys.path.append("..")
import integrators.csvIntegrator as csvIntgrtr
from cleaners.ieaTxtCleaner import clean as cleanIeaTxt
from cleaners.rystadCleaner import clean as rystadCleaner
from cleaners.clipperFloatingStorageCleaner import clean as clipperFloatingStorageCleaner
import os
from io import StringIO
import pandas as pd

class TestCsvIntegrator(unittest.TestCase):
    def setUp(self):
        self.ieaSupplyIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Supply',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='supply',
            file_columns=['Country','Product','Period','Quantity'],
            table_columns=['Country','Product','Period','PeriodType','Quantity','Asof'],
            file_name='SUPPLY.TXT',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter='\s+',
            truncate=False,
            column_for_delete='Asof',
        )

        self.ieaStockdatIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Stock Data',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='stockdat',
            file_columns=['Stock','Country','Product','Period','Quantity'],
            table_columns=['Stock','Country','Product','Period','PeriodType','Quantity','Asof'],
            file_name='stockdat.TXT',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter='\s+',
            truncate=False,
            column_for_delete='Asof',
        )

        self.ieaSplitdatIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Split Data',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='splitdat',
            file_columns=['Product','Country','Balance','Period','Quantity'],
            table_columns=['Product','Country','Balance','Period','PeriodType','Quantity','Asof'],
            file_name='splitdat.TXT',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter='\s+',
            truncate=False,
            column_for_delete='Asof',
        )

        self.ieaSummaryIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Summary',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='Summary',
            file_columns=['Geography','Final','Period','Quantity'],
            table_columns=['Geography','Final','Period','PeriodType','Quantity','Asof'],
            file_name='Summary.TXT',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter='\s+',
            truncate=False,
            column_for_delete='Asof',
        )

        self.ieaNOECDDEIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA NOECDDE',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='NOECDDE',
            file_columns=['Country','Period','Quantity'],
            table_columns=['Country','Period','PeriodType','Quantity','Asof'],
            file_name='NOECDDE.TXT',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter='\s+',
            truncate=False,
            column_for_delete='Asof',
        )

        self.ieaOECDDEIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA OECDDE',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='OECDDE',
            file_columns=['Country','PRODUCT','Period','Quantity'],
            table_columns=['Country','Product','Period','PeriodType','Quantity','Asof'],
            file_name='OECDDE.TXT',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter='\s+',
            truncate=False,
            column_for_delete='Asof',
        )

        self.ieaCrudeDataIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Crude Data',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='CRUDEDAT',
            file_columns=['COUNTRY','PRODUCT','BALANCE','Period','Quantity'],
            table_columns=['Country','Product','Balance','Period','PeriodType','Quantity','Asof'],
            file_name='CRUDEDAT.TXT',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter='\s+',
            truncate=False,
            column_for_delete='Asof',
        )

        self.ieaExportDataIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Export Data',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='ExportDAT',
            file_columns=['COUNTRY','PRODUCT','Export Country','Period','Quantity'],
            table_columns=['Country','Product','Export Country','Period','PeriodType','Quantity','Asof'],
            file_name='EXPORTDAT.TXT',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter='\s+',
            truncate=False,
            column_for_delete='Asof',
        )

        self.ieaImportDataIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Import Data',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='ImportDAT',
            file_columns=['COUNTRY','PRODUCT','Import Country','Period','Quantity'],
            table_columns=['Country','Product','Import Country','Period','PeriodType','Quantity','Asof'],
            file_name='ImPORTDAT.TXT',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter='\s+',
            truncate=False,
            column_for_delete='Asof',
        )

        self.ieaProdDataIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Prod Data',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='ProdDAT',
            file_columns=['PRODUCT','COUNTRY','BALANCE','Period','Quantity'],
            table_columns=['Product','Country','Balance','Period','PeriodType','Quantity','Asof'],
            file_name='PRODDAT.TXT',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter='\s+',
            truncate=False,
            column_for_delete='Asof',
        )


        self.ieaFieldIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Fields',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='field_by_field',
            file_columns=['FIELD', 'COUNTRY','PRODUCT','ENVIRONMENT','TIME','FREQUENCY','TIMESTAMP','VALUE'],
            table_columns=['Field','Country','Product','Environment','Period','PeriodType','Value','Asof'],
            file_name='field_by_field.csv',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean = cleanIeaTxt,
            delimiter=',',
            truncate=False,
            column_for_delete='Asof',
        )

        self.ieaCountryDetailsIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Country Details',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='country_details',
            table_columns=['COUNTRY_CODE','COUNTRY_NAME','ISO_ALPHA_2','ISO_ALPHA_3'],
            file_name='country_details.csv',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            delimiter=',',
            truncate=True
        )

        self.ieaFieldDetailsIntegrator = csvIntgrtr.CsvIntegrator(
            name='IEA Field Details',
            server=os.environ['ADUB_DBServer'],
            database='IEAData',
            table_name='field_details',
            table_columns=['FIELD_CODE','FIELD_NAME','COUNTRY','GROUP_CODE','GROUP_NAME','PRODUCT','ENVIRONMENT'],
            file_name='field_details.csv',
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            delimiter=',',
            truncate=True
        )

        self.rystadIntegrator = csvIntgrtr.CsvIntegrator(
            name='Rystad Production',
            server=os.environ['ADUB_DBServer'],
            database='Analytics',
            table_name='yview_RystadProduction',
            table_columns=['Country','Category','Grade','Period','Production','SulphurGroup','SulphurDetail','CleanSulphurDetail'],
            file_path="{}\\RystadProduction".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\RystadProduction\\".format(os.environ['ADUB_Import_Output_UNC']), 
            truncate=False,
            column_for_delete='Period',
            clean = rystadCleaner,
            delimiter=','
        )

        self.clipperFloatingStorageIntegrator = csvIntgrtr.CsvIntegrator(
            name='Clipper Floating Storage',
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

        self.mcQuillingIntegrator = csvIntgrtr.CsvIntegrator(
                name='McQuilling Assessments',
                server=os.environ['ADUB_DBServer'],
                database='Price',
                table_name='import.McQuilling',
                table_columns=['DateStamp','Class','Voyage','Tons','WS','TCE','Demurrage','Comments', 'VoyageType', 'IsDirty'],
                file_path="{}\\McQuilling".format(os.environ['ADUB_Import_Path']), 
                output_file_path="{}\\McQuilling\\".format(os.environ['ADUB_Import_Output_UNC']), 
                truncate=False,
                delimiter='|',
                column_for_delete='DateStamp',
        )
    
    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaSupplyIntegrator
    def test_run_ieaSupplyIntegrator(self):
        self.ieaSupplyIntegrator.run(os.path.join(self.ieaSupplyIntegrator.file_path, 'Supply.txt'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaStockdatIntegrator
    def test_run_ieaStockdatIntegrator(self):
        self.ieaStockdatIntegrator.run(os.path.join(self.ieaStockdatIntegrator.file_path, 'Stockdat.txt'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaSplitdatIntegrator
    def test_run_ieaSplitdatIntegrator(self):
        self.ieaSplitdatIntegrator.run(os.path.join(self.ieaSplitdatIntegrator.file_path, 'Splitdat.txt'))
    
    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaSummaryIntegrator
    def test_run_ieaSummaryIntegrator(self):
        self.ieaSummaryIntegrator.run(os.path.join(self.ieaSummaryIntegrator.file_path, 'SUMMARY.txt'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaNOECDDEIntegrator
    def test_run_ieaNOECDDEIntegrator(self):
        self.ieaNOECDDEIntegrator.run(os.path.join(self.ieaNOECDDEIntegrator.file_path, 'NOECDDE.txt'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaOECDDEIntegrator
    def test_run_ieaOECDDEIntegrator(self):
        self.ieaOECDDEIntegrator.run(os.path.join(self.ieaOECDDEIntegrator.file_path, 'OECDDE.txt'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaCrudeDataIntegrator
    def test_run_ieaCrudeDataIntegrator(self):
        self.ieaCrudeDataIntegrator.run(os.path.join(self.ieaCrudeDataIntegrator.file_path, 'CRUDEDAT.txt'))
    
    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaExportDataIntegrator
    def test_run_ieaExportDataIntegrator(self):
        self.ieaExportDataIntegrator.run(os.path.join(self.ieaExportDataIntegrator.file_path, 'ExportDAT.txt'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaImportDataIntegrator
    def test_run_ieaImportDataIntegrator(self):
        self.ieaImportDataIntegrator.run(os.path.join(self.ieaImportDataIntegrator.file_path, 'ImportDAT.txt'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaProdDataIntegrator
    def test_run_ieaProdDataIntegrator(self):
        self.ieaProdDataIntegrator.run(os.path.join(self.ieaProdDataIntegrator.file_path, 'PRODDAT.txt'))



    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaFieldIntegrator
    def test_run_ieaFieldIntegrator(self):
        self.ieaFieldIntegrator.run(os.path.join(self.ieaFieldIntegrator.file_path, 'field_by_field.csv'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaCountryDetailsIntegrator
    def test_run_ieaCountryDetailsIntegrator(self):
        self.ieaCountryDetailsIntegrator.run(os.path.join(self.ieaCountryDetailsIntegrator.file_path, 'country_details.csv'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_ieaFieldDetailsIntegrator
    def test_run_ieaFieldDetailsIntegrator(self):
        self.ieaFieldDetailsIntegrator.run(os.path.join(self.ieaFieldDetailsIntegrator.file_path, 'field_details.csv'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_rystad
    def test_run_rystad(self):            
        self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2013.csv'))
        # self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2014.csv'))
        # self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2015.csv'))
        # self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2016.csv'))
        # self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2017.csv'))
        # self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2018.csv'))
        # self.rystadIntegrator.run(os.path.join(self.rystadIntegrator.file_path, 'Rystad Production_2019.csv'))

     # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_mcQuilling
    def test_run_mcQuilling(self):            
        self.mcQuillingIntegrator.run(os.path.join(self.mcQuillingIntegrator.file_path, 'McQuilling_20191230.csv'))

    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test_run_clipperFloatingStorage
    def test_run_clipperFloatingStorage(self):            
        self.clipperFloatingStorageIntegrator.run(os.path.join(self.clipperFloatingStorageIntegrator.file_path, 'ClipperData Global Crude Oil Floating Storage (3).csv'))
    
    # python -m unittest test_CsvIntegrator.TestCsvIntegrator.test__arrangeColumns__
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
        dfResult = self.ieaSupplyIntegrator.__arrangeColumns__(df, table_columns)
        print(dfResult)
        self.assertEqual(dfResult.columns.tolist(), table_columns)

if(__name__ == '__main__'):
    unittest.main() 