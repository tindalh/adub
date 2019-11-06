import time
import datetime
import logging
from multiprocessing import Process
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

    # def on_any_event(self, event):
    #     print(event)

    def on_modified(self, event):
        diff = int(time.time()) - int(self.lastModified.get(event.src_path, time.time() - 10))
        
        if (os.path.isfile(event.src_path) and diff > 9):            
            while True:
                try:
                    new_path= event.src_path + "_"
                    os.rename(event.src_path,new_path)
                    os.rename(new_path,event.src_path)
                    time.sleep(0.05)
                    break
                except OSError as e:
                    time.sleep(0.05)

            self.lastModified[event.src_path] = time.time()        
            print("File {} {}. Running importer".format(event.src_path, event.event_type))
            logging.warning("File {} {}. Running importer".format(event.src_path, event.event_type))
            
            try:
                self.functionToRun(event.src_path.split('\\')[-1]) 
            except Exception as e:
                print(str(e))
                logging.error('Error in import watcher: {}'.format(str(e)))
                sendEmail('Error', 'import watcher', str(e))

class ImportWatcher(object):
    def __init__(self, importer, name):
        self.importer = importer
        self.name = name

    def startDirectoryWatch(self):

        logging.warning("Scheduling job for {} on {} for {} database. Input file name: {}"
            .format(self.name, self.importer.server, self.importer.database, self.importer.rawDataFilePath))

        event_handler = WatchdogHandler(self.importer.run)
        observer = Observer()
        observer.schedule(event_handler, path=self.importer.rawDataFilePath, recursive=False)
        observer.start()
        while True:
            try:
                time.sleep(0.05)
            except KeyboardInterrupt:
                break


    def watch(self):   
        self.process = Process(target=self.startDirectoryWatch)
        self.process.start()  
        