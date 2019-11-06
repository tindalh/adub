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

    def updateEiaSTOE(self):        
        logging.warning("Running EIA update for STOE")
        try:
            self.importer.runSeries('STEO.NYWSTEO.M')

            dataAccess = dtAccss.DataAccess(self.importer.server, self.importer.database)
            dataAccess.executeStoredProcedure('build_mview_EIASeries', ('STEO.NYWSTEO.M',))
            sendEmail('Info', 'Eia Scheduler', 'Short Term energy outlook data has been updated')
        except Exception as e:
            subject = 'Eia Scheduler Error'
            msg = 'Error: ' + str(e)
            logging.error('{}:{}'.format(subject, msg))
            sendEmail('Error', subject, msg)

    def runSchedule(self):
        logging.warning("Scheduling job for EIA update on {} for {} database"
            .format(self.importer.server, self.importer.database))

        schedule.every().wednesday.at("17:15").do(self.updateEiaSTOE)
        #schedule.every(1).minutes.do(self.updateEiaSTOE)
        while True:
            schedule.run_pending()
            time.sleep(1)


    def schedule(self):
        self.process = Process(target=self.runSchedule)
        self.process.start()  