import logging
import os

from constants import LOG_FILE, INTERNAL_DOMAIN, PRODUCTION_DB_SERVER
import helpers.analyticsEmail as analyticsEmail

logging.basicConfig(filename=LOG_FILE,level=logging.INFO,format='%(asctime)s %(message)s')

def log(moduleName, functionName, message, level='Info', email=False, emailSubject="", emailTable=""):
    
    logMessage = "{:<8}".format(level) + " | " + moduleName + "." + "{:<50s}".format(functionName) + " | " + message
    
    print(logMessage)
    
    if(level.lower() == 'info'):
        logging.info(logMessage)
    elif(level.lower() == 'warning'):
        logging.warning(logMessage)
    elif(level.lower() == 'error'):
        logging.error(logMessage)

    if(email and 'arcpet' in os.environ["userdomain"].lower() ):
        if(os.environ['ADUB_DBServer'].lower() != PRODUCTION_DB_SERVER):
            emailSubject = 'TEST ' + emailSubject
            
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

def error_email(moduleName, functionName, message):
    logMessage = "{:<8}".format("Error") + " | " + moduleName + "." + "{:<50s}".format(functionName) + " | " + message
    print(logMessage)

    logging.error(logMessage)

    if(INTERNAL_DOMAIN.lower() in os.environ["userdomain"].lower() ):
        if(os.environ['ADUB_DBServer'].lower() != PRODUCTION_DB_SERVER):
            emailSubject = 'TEST ' + emailSubject
            
        analyticsEmail.sendEmail("Error", moduleName + '.' + functionName, message)

    return logMessage

    

    