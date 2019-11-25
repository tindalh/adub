import serviceTasks.brokerReceiver as brkrRcvr
import serviceTasks.importWatcher as wtchr
import serviceTasks.jobScheduler as jobSchdlr
import datetime
import logging
import copy
import os
import sys
import schedule
sys.path.insert(1, 'C:\\Apps\\Analytics\\adimport')
import franchisers.refineryInfoFranchiser as refineryInfoFrnchsr
import cleaners.ieaCleaner as cleanIea
import cleaners.rystadCleaner as cleanRystad
import cleaners.clipperFloatingStorageCleaner as cleanClipperFloatingStorage
import integrators.csvIntegrator as csvImprtr
import importers.eiaImporter as eiaImprtr
#import importers.clipperFloatingStorageImporter as clipper
from analyticsEmail import sendEmail


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
        # self.clipperFloatingStorageImporter = clipper.ClipperFloatingStorageImporter(
        #     server=os.environ['ADUB_DBServer'], 
        #     database='STG_Targo', 
        #     tempCSVFilePath="{}\\ClipperFloatingStorage".format(os.environ['ADUB_Import_Output_UNC']),
        #     rawDataFilePath="{}\\ClipperFloatingStorage\\".format(os.environ['ADUB_Import_Path']), 
        #     bulkInsertFilePath="{}\\ClipperFloatingStorage".format(os.environ['ADUB_Import_Output']),
        #     lastUpdate=-80
        # )
        # self.rystadImporter = rystad.RystadImporter(
        #     server=os.environ['ADUB_DBServer'], 
        #     database="Analytics",
        #     tempCSVFilePath="{}\\RystadProduction".format(os.environ['ADUB_Import_Output_UNC']), 
        #     rawDataFilePath="{}\\RystadProduction\\".format(os.environ['ADUB_Import_Path']), 
        #     bulkInsertFilePath="{}\\RystadProduction".format(os.environ['ADUB_Import_Output'])
        # )
        self.ieaIntegrator = csvImprtr.CsvIntegrator(
            name='IEA Integrator',
            server=os.environ['ADUB_DBServer'], 
            database="IEAData",
            file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
            clean=cleanIea.clean
        )
        self.rystadIntegrator = csvImprtr.CsvIntegrator(
            name='Rystad Integrator',
            server=os.environ['ADUB_DBServer'], 
            database="Analytics",
            table_name='yview_RystadProduction',
            file_path="{}\\RystadProduction".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\RystadProduction\\".format(os.environ['ADUB_Import_Output_UNC']), 
            truncate=False,
            column_for_delete='Period',
            clean=cleanRystad.clean
        )
        self.clipperFloatingStorageIntegrator = csvImprtr.CsvIntegrator(
            name='Clipper Floating Storage Importer',
            server=os.environ['ADUB_DBServer'], 
            database="STG_Targo",
            file_path="{}\\ClipperFloatingStorage".format(os.environ['ADUB_Import_Path']), 
            output_file_path="{}\\ClipperFloatingStorage\\".format(os.environ['ADUB_Import_Output_UNC']), 
            truncate=False,
            table_name='yview_ClipperFloatingStorage',
            column_for_delete='date_asof',
            clean = cleanClipperFloatingStorage.clean,
            clean_arg='2015-01-01'
        )
        self.refineryInfoFranchiser = refineryInfoFrnchsr.RefineryInfoFranchiser(
            os.environ['ADUB_DBServer'], 'RefineryInfo'
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
        wtchr.watch('Rystad Production', self.rystadIntegrator.run, self.rystadIntegrator.file_path)

    def startIeaWatcher(self):
        wtchr.watch('IEA Data', self.ieaIntegrator.run, self.ieaIntegrator.file_path)

    def startClipperFloatingStorageWatcher(self):
        wtchr.watch('Clipper Floating Storage', self.clipperFloatingStorageIntegrator.run, self.clipperFloatingStorageIntegrator.file_path)

    def startEiaImportScheduler(self):
        scheduler = schedule.every().wednesday.at("20:00").do(self.eiaImporter.runSeries).scheduler
        eiaScheduler = jobSchdlr.JobScheduler('Eia Import', scheduler)
        eiaScheduler.schedule()  
