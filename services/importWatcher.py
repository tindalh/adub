import time
import datetime
import logging
from multiprocessing import Process
import os
import fnmatch
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from helpers.log import info, error_email

def watch(name, importer=None, fn=None, file_path=None, file_name=None):  
    """
        Entry point for this module
    """ 
    if(importer is not None):
        process = Process(target=__startDirectoryWatch__, args=(name, importer.run, importer.file_path, importer.file_name))
    else:
        process = Process(target=__startDirectoryWatch__, args=(name, fn, file_path, file_name))

    process.start() 

def __startDirectoryWatch__(name, importerFunc, file_path, file_name = None):
    info(__name__, 'startDirectoryWatch', f"Watching {name} in {file_path}")
    
    event_handler = WatchdogHandler(importerFunc, file_name)
    observer = Observer()
    observer.schedule(event_handler, path=file_path, recursive=False)
    observer.start()
    while True:
        try:
            time.sleep(0.05)
        except KeyboardInterrupt:
            break

class WatchdogHandler(FileSystemEventHandler):
    def __init__(self, functionToRun, file_name = None):
        self.functionToRun = functionToRun
        self.lastModified = {}
        self.file_name = file_name
        FileSystemEventHandler.__init__(self)        

    def on_modified(self, event):
        try:
            if(self.file_name and event.src_path.split('\\')[-1].lower() != self.file_name.lower()):            
                return

            diff = int(time.time()) - int(self.lastModified.get(event.src_path, time.time() - 10))
            if (os.path.isfile(event.src_path) and diff > 9):            
                while True:
                    try:
                        new_path= event.src_path + "_"
                        os.rename(event.src_path,new_path)
                        os.rename(new_path,event.src_path)
                        time.sleep(0.3)
                        break
                    except OSError as e:
                        time.sleep(0.05)

                self.lastModified[event.src_path] = time.time()   
              
                self.functionToRun(event.src_path) 
        except Exception as e:
            error_email(__name__, 'on_modified', f"Import Watcher error: {str(e)}")

    
        