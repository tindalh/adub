import schedule
import time
import sys
import logging
from multiprocessing import Process
import helpers.dataAccess as dtAccss
from helpers.log import info, error_email

class JobScheduler(object):
    def __init__(self, name, scheduler):
        self.name = name
        self.scheduler = scheduler

    def run(self):
        info(__name__, 'run', f"Scheduling job for {self.name}")

        try:
            while True:            
                self.scheduler.run_pending()
                time.sleep(1)
        except Exception as e:
            error_email(__name__, 'run', f"Error running scheduled job for {self.name}:\n{str(e)}")

    def schedule(self):
        self.process = Process(target=self.run)
        self.process.start()  