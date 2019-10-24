import time
import datetime
import logging
import os
import fnmatch
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import sys
sys.path.insert(1,"C:\\Apps\\Analytics\\common")
from analyticsEmail import sendEmail

class WatchdogHandler(FileSystemEventHandler):
    def __init__(self, functionToRun):
        self.functionToRun = functionToRun
        self.lastModified = {}
        FileSystemEventHandler.__init__(self)

    def on_modified(self, event):

        diff = int(time.time()) - int(self.lastModified.get(event.src_path, time.time() - 3))

        if (os.path.isfile(event.src_path) and diff > 2):
            logging.warning("File {} {}. Running importer".format(event.src_path, event.event_type))
            try:
                self.functionToRun(event.src_path.split('\\')[-1]) 
                self.lastModified[event.src_path] = time.time()
            except Exception as e:
                print(str(e))
                logging.error('Error in checkRystad: {}'.format(str(e)))
                sendEmail('Error', 'Rystad Import', str(e))

class ImportWatcher(object):
    def startDirectoryWatch(self, importer):

        logging.warning("Scheduling job for {} on {} for {} database. Input file name: {}"
            .format(importer.whoFrom, importer.server, importer.database, importer.rawDataFilePath))

        event_handler = WatchdogHandler(importer.run)
        observer = Observer()
        observer.schedule(event_handler, path=importer.rawDataFilePath, recursive=False)
        observer.start()
