import logging

import helpers.analyticsEmail as analyticsEmail

logging.basicConfig(filename='C:\\Apps\\Analytics\\adub\\logs\\adub.log',level=logging.INFO,format='%(asctime)s %(message)s')

def log(moduleName, functionName, message, level='Info', email=False, emailSubject="", emailTable=""):
    
    logMessage = "{:<8}".format(level) + " | " + moduleName + "." + "{:<50s}".format(functionName) + " | " + message
    
    print(logMessage)
    
    if(level.lower() == 'info'):
        logging.info(logMessage)
    elif(level.lower() == 'warning'):
        logging.warning(logMessage)
    elif(level.lower() == 'error'):
        logging.error(logMessage)

    if(email):
        analyticsEmail.sendEmail(level, emailSubject, message, emailTable)

    return logMessage

    