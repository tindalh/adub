import logging

from constants import LOG_FILE
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

def info(moduleName, functionName, message):
    logMessage = "{:<8}".format("Info") + " | " + moduleName + "." + "{:<50s}".format(functionName) + " | " + message
    print(logMessage)

    logging.info(logMessage)
    return logMessage

def warning(moduleName, functionName, message):
    logMessage = "{:<8}".format("Warning") + " | " + moduleName + "." + "{:<50s}".format(functionName) + " | " + message
    print(logMessage)

    logging.warning(logMessage)
    return logMessage

def error(moduleName, functionName, message):
    logMessage = "{:<8}".format("Error") + " | " + moduleName + "." + "{:<50s}".format(functionName) + " | " + message
    print(logMessage)

    logging.error(logMessage)
    return logMessage

    

    