
from serviceBase import SMWinservice
import initialiser as intlsr
import logging
import time

logging.basicConfig(filename='C:\\Apps\\adub\\logs\\adub.log',level=logging.WARNING,format='%(asctime)s %(message)s')

class Adub(SMWinservice):    
    _svc_name_ = "adub"
    _svc_display_name_ = "Analytics Data Hub"
    _svc_description_ = "Manages the Analytics Infrastructure"    

    def start(self):
        logging.warning('Service start')
        self.isrunning = True

    def stop(self):
        logging.warning('Service stop')
        self.isrunning = False

    def main(self):
        initialiser = intlsr.Initialiser()
        analyticsHubBrokerReceiver = initialiser.startAnalyticsHubReceiving()
        rystadImportSchedule = initialiser.startRystadWatcher()        

        while True:
            try:
                time.sleep(2)
                if(not self.isrunning):
                    logging.warning('Service has been stopped')
                    break
            except KeyboardInterrupt:
                logging.warning('Service has now stopped')
                analyticsHubBrokerReceiver.stopConsuming()
                break

if __name__ == '__main__':
    Adub.parse_command_line()
