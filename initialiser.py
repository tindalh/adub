import serviceTasks.brokerReceiver as brkrRcvr
import serviceTasks.importWatcher as wtchr
import serviceTasks.jobScheduler as jobSchdlr
import datetime
import logging
import copy
import os
import sys
import schedule
import franchisers.refineryInfoFranchiser as refineryInfoFrnchsr
import integrators.csvIntegrator as csvIntgrtr
from importers.mcQuilling import McQuilling
from cred_secrets import USERNAME, PASSWORD
from cleaners.ieaTxtCleaner import clean as cleanIeaTxt
from cleaners.rystadCleaner import clean as rystadCleaner
from cleaners.clipperFloatingStorageCleaner import clean as clipperFloatingStorageCleaner
import importers.eiaImporter as eiaImprtr
from helpers.analyticsEmail import sendEmail

EXCHANGE_SERVER = 'https://loneca.arcpet.co.uk/EWS/Exchange.asmx'
EMAIL_ADDRESS = 'henryt@arcpet.co.uk'
class Initialiser(object):
    def __init__(self):
        self.eiaImporter = eiaImprtr.EiaImporter(
            server=os.environ['ADUB_DBServer'], 
            database='Analytics',
            url="http://api.eia.gov",
            api_key='a50a785e3c8ad1b5bdd26cf522d4d473',
            file_path="{}\\EIA".format(os.environ['ADUB_Import_Output_UNC']),
            bulkinsert_path="{}\\EIA".format(os.environ['ADUB_Import_Output'])
        )

        
        self.mcQuilling = McQuilling(
            'Daily Freight Rate Assessment',
            "{}\\McQuilling".format(os.environ['ADUB_Import_Path']),
            database_server=os.environ['ADUB_DBServer'],
            database='Price'
        )
     
        self.refineryInfoFranchiser = refineryInfoFrnchsr.RefineryInfoFranchiser(
            os.environ['ADUB_DBServer'], 'RefineryInfo'
        )

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
        

    def startRefineryInfoFranchisingBrokerReceiving(self):
        refineryInfoBrokerReceiver = brkrRcvr.BrokerReceiver(
            queue='buildRefineryViews', 
            database='RefineryInfo',
            job=self.refineryInfoFranchiser.run
        )
        refineryInfoBrokerReceiver.run()

        return refineryInfoBrokerReceiver

    def startEIAUpdateBrokerReceiving(self):
        eiaUpdateBrokerReceiver = brkrRcvr.BrokerReceiver(
            queue='eiaUpdate', 
            database='Analytics',
            job=self.eiaImporter.runSeries
        )
        eiaUpdateBrokerReceiver.run()

    def startRystadWatcher(self):
        wtchr.watch('Rystad Production', self.rystadIntegrator)

    def startMcQuillingWatcher(self):
        wtchr.watch('McQuilling Assessments', self.mcQuillingIntegrator)

    def startIeaWatchers(self):
        wtchr.watch('IEA Supply', self.ieaSupplyIntegrator)
        wtchr.watch('IEA Stock Data', self.ieaStockdatIntegrator)
        wtchr.watch('IEA Split Data', self.ieaSplitdatIntegrator)
        wtchr.watch('IEA Summary', self.ieaSummaryIntegrator)
        wtchr.watch('IEA NOECDDE', self.ieaNOECDDEIntegrator)
        wtchr.watch('IEA OECDDE', self.ieaOECDDEIntegrator)
        wtchr.watch('IEA Crude Data', self.ieaCrudeDataIntegrator)
        wtchr.watch('IEA Export Data', self.ieaExportDataIntegrator)
        wtchr.watch('IEA Import Data', self.ieaImportDataIntegrator)
        wtchr.watch('IEA Prod Data', self.ieaProdDataIntegrator)
        wtchr.watch('IEA Fields', self.ieaFieldIntegrator)
        wtchr.watch('IEA Country Details', self.ieaCountryDetailsIntegrator)
        wtchr.watch('IEA Field Details', self.ieaFieldDetailsIntegrator)

    def startClipperFloatingStorageWatcher(self):
        wtchr.watch('Clipper Floating Storage', self.clipperFloatingStorageIntegrator)

    def startEiaImportScheduler(self):
        #scheduler = schedule.every().minutes.do(self.eiaImporter.runSeries).scheduler
        scheduler = schedule.every().wednesday.at("20:00").do(self.eiaImporter.runSeries).scheduler
        eiaScheduler = jobSchdlr.JobScheduler('Eia Import', scheduler)
        eiaScheduler.schedule()  

    def startMcQuillingImportScheduler(self):
        #scheduler = schedule.every(1).minutes.do(self.mcQuilling.run, USERNAME,PASSWORD,EXCHANGE_SERVER,EMAIL_ADDRESS).scheduler
        scheduler = schedule.every(1).day.at("14:00").do(self.mcQuilling.run, USERNAME,PASSWORD,EXCHANGE_SERVER,EMAIL_ADDRESS).scheduler
        mcQuillingScheduler = jobSchdlr.JobScheduler('McQuilling Import', scheduler)
        mcQuillingScheduler.schedule()  
