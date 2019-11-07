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
import importers.rystadImporter as rystad
import importers.eiaImporter as eia
import importers.clipperFloatingStorageImporter as clipper
from analyticsEmail import sendEmail


class Initialiser(object):
    def __init__(self):
        self.eiaImporter = eia.EiaImporter(
            server=os.environ['ADUB_DBServer'], 
            database='Analytics',
            url="http://api.eia.gov",
            api_key='a50a785e3c8ad1b5bdd26cf522d4d473',
            file_path="{}\\EIA".format(os.environ['ADUB_Import_Output_UNC']),
            bulkinsert_path="{}\\EIA".format(os.environ['ADUB_Import_Output'])
        )
        self.clipperFloatingStorageImporter = clipper.ClipperFloatingStorageImporter(
            server=os.environ['ADUB_DBServer'], 
            database='STG_Targo', 
            tempCSVFilePath="{}\\ClipperFloatingStorage".format(os.environ['ADUB_Import_Output_UNC']),
            rawDataFilePath="{}\\ClipperFloatingStorage\\".format(os.environ['ADUB_Import_Path']), 
            bulkInsertFilePath="{}\\ClipperFloatingStorage".format(os.environ['ADUB_Import_Output']),
            lastUpdate=-80
        )
        self.rystadImporter = rystad.RystadImporter(
            server=os.environ['ADUB_DBServer'], 
            database="Analytics",
            tempCSVFilePath="{}\\RystadProduction".format(os.environ['ADUB_Import_Output_UNC']), 
            rawDataFilePath="{}\\RystadProduction\\".format(os.environ['ADUB_Import_Path']), 
            bulkInsertFilePath="{}\\RystadProduction".format(os.environ['ADUB_Import_Output'])
        )
        self.refineryInfoFranchiser = refineryInfoFrnchsr.RefineryInfoFranchiser(
            os.environ['ADUB_DBServer'], 'RefineryInfo'
        )

    def isValidEnvironment(self, process, jobType='notCsv'):
        valid = True
        environmentVarMissing = 'ADUB_DBServer'
        
        if(os.environ.get('ADUB_DBServer', 'e') == 'e'):
            valid = False

        if(jobType == 'csv'):
            if(os.environ.get('ADUB_Import_Path', 'e')) == 'e':
                environmentVarMissing += ', ADUB_Import_Path'
                valid = False
            if(os.environ.get('ADUB_Import_Output_UNC', 'e')) == 'e':
                environmentVarMissing += ', ADUB_Import_Output_UNC'
                valid = False
            if(os.environ.get('ADUB_Import_Output', 'e')) == 'e':
                environmentVarMissing += ', ADUB_Import_Output'
                valid = False

        if(not valid):
            subject = '{} Error - Environment Variables'.format(process)
            msg = '{} must be defined on the machine'.format(environmentVarMissing)
            logging.error('{}:{}'.format(subject, msg))
            sendEmail('Error', subject, msg)
        
        return valid

    def startRefineryInfoFranchisingBrokerReceiving(self):
        if(self.isValidEnvironment('Refinery Info Broker Receiver')):

            refineryInfoBrokerReceiver = brkrRcvr.BrokerReceiver(
                queue='buildRefineryViews', 
                database='RefineryInfo',
                job=self.refineryInfoFranchiser.run
            )
            refineryInfoBrokerReceiver.run()

        return refineryInfoBrokerReceiver

    def startEIAUpdateBrokerReceiving(self):
        if(self.isValidEnvironment('EIA Update Broker Receiver' )):

            eiaUpdateBrokerReceiver = brkrRcvr.BrokerReceiver(
                queue='eiaUpdate', 
                database='Analytics',
                job=self.eiaImporter.runSeries
            )
            eiaUpdateBrokerReceiver.run()

    def startRystadWatcher(self):
        if(self.isValidEnvironment('csv')):
            rystadWatcher = wtchr.ImportWatcher(self.rystadImporter, 'Rystad Production')
            rystadWatcher.watch()

    def startClipperFloatingStorageWatcher(self):
        if(self.isValidEnvironment('csv')):            
            clipperFloatingStorageWathcer = wtchr.ImportWatcher(self.clipperFloatingStorageImporter, 'Clipper Floating Storage')
            clipperFloatingStorageWathcer.watch()  

    def startEiaImportScheduler(self):
        if(self.isValidEnvironment('Eia Import Scheduler')):            
            scheduler = schedule.every().wednesday.at("20:00").do(self.eiaImporter.runSeries).scheduler
            eiaScheduler = jobSchdlr.JobScheduler('Eia Import', scheduler)
            eiaScheduler.schedule()  
