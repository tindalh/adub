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
from cleaners.ieaCleaner import clean as cleanIea
from cleaners.rystadCleaner import clean as rystadCleaner
from cleaners.clipperFloatingStorageCleaner import clean as clipperFloatingStorageCleaner
import importers.eiaImporter as eiaImprtr
from helpers.analyticsEmail import sendEmail


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
        #scheduler = schedule.every().minutes.do(self.eiaImporter.runSeries).scheduler
        scheduler = schedule.every().wednesday.at("20:00").do(self.eiaImporter.runSeries).scheduler
        eiaScheduler = jobSchdlr.JobScheduler('Eia Import', scheduler)
        eiaScheduler.schedule()  
