import schedule
import time
import sys
import logging
from multiprocessing import Process
sys.path.insert(1, 'C:\\Apps\\Analytics\\common')
import dataAccess as dtAccss
from analyticsEmail import sendEmail

class EiaScheduler(object):
    def __init__(self, importer):
        self.importer = importer

    def updateEia(self):        
        logging.warning("Running EIA update")
        try:
            dataAccess = dtAccss.DataAccess(self.importer.server, self.importer.database)
            seriesToUpdate = dataAccess.load('EIASeriesShortname')

            for s in seriesToUpdate:
                print('Loading updates for {}'.format(s[1]))
                self.importer.runSeries(s[1])
                dataAccess.executeStoredProcedure('build_mview_EIASeries', (s[1],))
            print('Complete')
            sendEmail('Info', 'Eia Scheduler', 'Weekly data has been updated')
        except Exception as e:
            subject = 'Eia Scheduler Error'
            msg = 'Error: ' + str(e)
            logging.error('{}:{}'.format(subject, msg))
            sendEmail('Error', subject, msg)

    def runSchedule(self):
        logging.warning("Scheduling job for EIA update on {} for {} database"
            .format(self.importer.server, self.importer.database))
        schedule.every().wednesday.at("23:00").do(self.updateEia)
        while True:
            schedule.run_pending()
            time.sleep(1)


    def schedule(self):
        self.process = Process(target=self.runSchedule)
        self.process.start()  