from exchangelib import FileAttachment, FaultTolerance, Configuration, Credentials, Account, DELEGATE, EWSDateTime
from exchangelib.folders import ArchiveInbox
from exchangelib.version import EXCHANGE_2016


import datetime
import time
import sys
import os
sys.path.append('..')
from helpers.utils import insert_datestamp_in_filename
from helpers.log import error_email

from cred_secrets import USERNAME, PASSWORD
from constants import ANALYTICS_EMAIL_ADDRESS, EXCHANGE_SERVER


def account():
    config = Configuration(
        service_endpoint=EXCHANGE_SERVER,
        credentials=Credentials(
            USERNAME, 
            PASSWORD
        )
    )

    account = Account(
        ANALYTICS_EMAIL_ADDRESS, 
        autodiscover=False,
        config=config, 
        access_type=DELEGATE
    )
    return account

# datetime.datetime -> exchangelib.EWSDate
# Consumes a date and produces a localized start date
def get_start_date(account, date):
    return account.default_timezone.localize(EWSDateTime(date.year, date.month, date.day))

# String datetime.datetime -> List of exchangelib.Message
# Consumes an email subject and a start date and produces a list of emails not longer than 100
def get_emails(account, email_subject, date):
    start = get_start_date(account, date)
    return account.inbox.filter(subject__contains=email_subject)\
        .filter(datetime_received__gt=start).order_by('-datetime_received')[:100]

# List of exchangelib.Message String -> None
# Consumes a list of emails and saves attachments
def save_email_attachments(emails, file_path, attachment_directories):

    if(len(emails) == 0):
        return None
    else:
        first = emails[0]
        rest = emails[1:]

        save_attachments(first.attachments, file_path, first.datetime_received, attachment_directories)
        
        save_email_attachments(rest, file_path, attachment_directories)

# List of exchangelib.FileAttachment String Date -> None
# Consumes a list of attachments and saves each one to file_path
def save_attachments(attachments, file_path, date_received, attachment_directories):
    for attachment in attachments:
        if isinstance(attachment, FileAttachment):
            save_attachment(attachment, file_path, date_received, attachment_directories)

# exchangelib.FileAttachment String Date -> None
# Consumes an attachment and saves it to file_path with the date stamp inserted into the filename
def save_attachment(attachment, file_path, date_received, attachment_directories):  
    full_file_path = get_file_path(file_path, attachment.name, date_received, attachment_directories)
    
    if (full_file_path is None):
        error_email(__name__, 'save_attachment',\
            f'The list of directories supplied does not match the email attachment name: {attachment.name}')
    else:
        with open(full_file_path, 'wb') as f:
            f.write(attachment.content)

def get_file_path(file_path, attachment_name, date_received, attachment_directories):
    full_file_path = None

    if(len(attachment_directories) == 0):
        full_file_path = insert_datestamp_in_filename(file_path, attachment_name, date_received)
    else:
        for d in attachment_directories:
            if d.lower() in attachment_name.lower():  
                full_file_path = insert_datestamp_in_filename(os.path.join(file_path, d), attachment_name, date_received)
    
    return full_file_path

if(__name__ == "__main__"):
    print(account())