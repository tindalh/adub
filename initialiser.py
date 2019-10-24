import serviceTasks.brokerReceiver as brkrRcvr
import serviceTasks.importWatcher as wtchr
import datetime
import logging
import os
import sys
sys.path.insert(1, 'C:\\Apps\\Analytics\\adimport')
import importers.rystadImporter as rystad
from analyticsEmail import sendEmail


class Initialiser(object):
    def isValidEnvironment(self, process):
        if(os.environ.get('ADUB_DBServer', 'e') == 'e'):
            print('server')
            return False

        if(process == 'rystadProduction'):
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
        if(not self.isValidEnvironment('rystadProduction')):
            sendEmail('Error', 'Environment Variables', 'ADUB_DBServer, ADUB_ImportPath and ADUB_ImportPath_UNC must be defined on the machine')
        else:
            rystadImporter = rystad.RystadImporter(
                args=None,
                whoFrom="rystadProduction", 
                server=os.environ['ADUB_DBServer'], 
                lastUpdate=-1, 
                database="Analytics",
                tempCSVFilePath=os.environ['ADUB_Import_Output_UNC'], 
                rawDataFilePath=os.environ['ADUB_Import_Path'], 
                bulkInsertFilePath=os.environ['ADUB_Import_Output']
            )
            rystadWatcher = wtchr.ImportWatcher()
            rystadWatcher.startDirectoryWatch(rystadImporter)

            return rystadWatcher
        return None