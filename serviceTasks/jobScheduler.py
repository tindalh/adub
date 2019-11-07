import schedule
import time
import sys
import logging
from multiprocessing import Process
sys.path.insert(1, 'C:\\Apps\\Analytics\\common')
import dataAccess as dtAccss
from analyticsEmail import sendEmail

class JobScheduler(object):
    def __init__(self, name, scheduler):
        self.name = name
        self.scheduler = scheduler

    def run(self):
        logging.warning("Scheduling job for {}".format(self.name))
        while True:
            self.scheduler.run_pending()
            time.sleep(1)

    def schedule(self):
        self.process = Process(target=self.run)
        self.process.start()  