import initialiser as intlsr
import logging

logging.basicConfig(filename='C:\\Apps\\adub\\logs\\adub.log',level=logging.WARNING,format='%(asctime)s %(message)s')

if __name__ == '__main__':
    initialiser = intlsr.Initialiser()
    analyticsHubBrokerReceiver = initialiser.startAnalyticsHubReceiving()
    rystadImportSchedule = initialiser.startRystadWatcher()      
