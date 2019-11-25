import schedule
import time
import sys
import logging
from multiprocessing import Process
import helpers.dataAccess as dtAccss
from helpers.log import log

class JobScheduler(object):
    def __init__(self, name, scheduler):
        self.name = name
        self.scheduler = scheduler

    def run(self):
        log(__name__, 'run', f"Scheduling job for {self.name}")
        while True:
            self.scheduler.run_pending()
            time.sleep(1)

    def schedule(self):
        self.process = Process(target=self.run)
        self.process.start()  