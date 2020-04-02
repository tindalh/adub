import smtplib
import os
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def sendEmail(type, subject, body, table=""):

    message = MIMEMultipart('alternative')    
    message['From'] = 'targo.support@arcpet.co.uk'

    if(type.lower() == 'info'):
        subjectPrefix = ''
        message['To'] = os.environ['ADUB_Email_To']
    elif(type.lower() == 'error'):
        subjectPrefix = 'Error - '
        message['To'] = 'henryt@arcpet.co.uk'
    message['Subject'] = '{}{}'.format(subjectPrefix,subject)


    msg = MIMEText(f"<html><body><p>Dear Analyst,</p><p>{body}</p>{table}</body></html>", 'html')
    message.attach(msg)

    

    try:
        smptServer = smtplib.SMTP('10.20.20.20')
        smptServer.send_message(message)
        smptServer.quit()
    except Exception as e:
        raise e

    