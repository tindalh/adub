import initialiser as intlsr
import logging

logging.basicConfig(filename='C:\\Apps\\Analytics\\adub\\logs\\adub.log',level=logging.WARNING,format='%(asctime)s %(message)s')

if __name__ == '__main__':
    initialiser = intlsr.Initialiser()
    initialiser.startAnalyticsHubReceiving()
    initialiser.startRystadWatcher()      
    initialiser.startClipperFloatingStorageWatcher()     
    initialiser.startEiaImportScheduler(); 

