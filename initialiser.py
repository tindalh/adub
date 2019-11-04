import serviceTasks.brokerReceiver as brkrRcvr
import serviceTasks.importWatcher as wtchr
import serviceTasks.eiaScheduler as eiaSchdlr
import datetime
import logging
import os
import sys
sys.path.insert(1, 'C:\\Apps\\Analytics\\adimport')
import importers.rystadImporter as rystad
import importers.eiaImporter as eia
import importers.clipperFloatingStorageImporter as clipper
from analyticsEmail import sendEmail


class Initialiser(object):
    def isValidEnvironment(self, process):
        if(os.environ.get('ADUB_DBServer', 'e') == 'e'):
            print('server')
            return False

        if(process == 'csv'):
            if(os.environ.get('ADUB_Import_Path', 'e')) == 'e':
                return False
            if(os.environ.get('ADUB_Import_Output_UNC', 'e')) == 'e':
                return False
            if(os.environ.get('ADUB_Import_Output', 'e')) == 'e':
                return False

        return True

    def startAnalyticsHubReceiving(self):
        analyticsHubBrokerReceiver = brkrRcvr.BrokerReceiver(
            queue='buildRefineryViews', 
            database='RefineryInfo', 
            queryToRun="'sp_build_RefineryViews'",
            params="(int(body), 700)"
        )
        analyticsHubBrokerReceiver.run()

        return analyticsHubBrokerReceiver

    def startRystadWatcher(self):
        if(not self.isValidEnvironment('csv')):
            sendEmail('Error', 'Environment Variables', 'ADUB_DBServer, ADUB_ImportPath and ADUB_ImportPath_UNC must be defined on the machine')
        else:
            rystadImporter = rystad.RystadImporter(
                server=os.environ['ADUB_DBServer'], 
                database="Analytics",
                tempCSVFilePath="{}\\RystadProduction".format(os.environ['ADUB_Import_Output_UNC']), 
                rawDataFilePath="{}\\RystadProduction\\".format(os.environ['ADUB_Import_Path']), 
                bulkInsertFilePath="{}\\RystadProduction".format(os.environ['ADUB_Import_Output'])
            )
            rystadWatcher = wtchr.ImportWatcher(rystadImporter, 'Rystad Production')
            rystadWatcher.watch()

            return rystadWatcher
        return None

    def startClipperFloatingStorageWatcher(self):
        if(not self.isValidEnvironment('csv')):
            sendEmail('Error', 'Environment Variables', 'ADUB_DBServer, ADUB_ImportPath and ADUB_ImportPath_UNC must be defined on the machine')
        else:
            clipperFloatingStorageImporter = clipper.ClipperFloatingStorageImporter(
                server=os.environ['ADUB_DBServer'], 
                database='STG_Targo', 
                tempCSVFilePath="{}\\ClipperFloatingStorage".format(os.environ['ADUB_Import_Output_UNC']),
                rawDataFilePath="{}\\ClipperFloatingStorage\\".format(os.environ['ADUB_Import_Path']), 
                bulkInsertFilePath="{}\\ClipperFloatingStorage".format(os.environ['ADUB_Import_Output']),
                lastUpdate=-80
            )
            clipperFloatingStorageWathcer = wtchr.ImportWatcher(clipperFloatingStorageImporter, 'Clipper Floating Storage')
            clipperFloatingStorageWathcer.watch()  

    def startEiaImportScheduler(self):
        if(not self.isValidEnvironment('scheduler')):
            sendEmail('Error', 'Environment Variables', 'ADUB_DBServer must be defined on the machine')
        else:
            eiaImporter = eia.EiaImporter(
                server=os.environ['ADUB_DBServer'], 
                database='Analytics',
                url="http://api.eia.gov",
                api_key='a50a785e3c8ad1b5bdd26cf522d4d473',
                file_path="{}\\EIA".format(os.environ['ADUB_Import_Output_UNC']),
                bulkinsert_path="{}\\EIA".format(os.environ['ADUB_Import_Output'])
            )
            eiaScheduler = eiaSchdlr.EiaScheduler(eiaImporter)
            eiaScheduler.schedule()  
