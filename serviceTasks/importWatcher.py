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
from log import log

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
            log(__name__, 'on_modified', f"Running importer for {event.src_path}") 
            
            try:
                self.functionToRun(event.src_path) 
            except Exception as e:
                log(__name__, 'on_modified', f"Error: {str(e)}", 'Error', True, 'ImportWatcher')

def startDirectoryWatch(name, importerFunc, file_path):
    log(__name__, 'startDirectoryWatch', f"Scheduling {name}")
    
    event_handler = WatchdogHandler(importerFunc)
    observer = Observer()
    observer.schedule(event_handler, path=file_path, recursive=False)
    observer.start()
    while True:
        try:
            time.sleep(0.05)
        except KeyboardInterrupt:
            break


def watch(name, importerFunc, file_path):   
    process = Process(target=startDirectoryWatch, args=(name, importerFunc, file_path))
    process.start() 

    
        