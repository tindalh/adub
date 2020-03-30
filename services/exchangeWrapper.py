from exchangelib import FileAttachment, FaultTolerance, Configuration, Credentials, Account, DELEGATE, EWSDateTime
import datetime
import time
import sys
import os
sys.path.append('..')
from helpers.utils import insert_datestamp_in_filename
from helpers.log import error_email

from cred_secrets import USERNAME, PASSWORD
from constants import ANALYTICS_EMAIL_ADDRESS, EXCHANGE_SERVER

class ExchangeWrapper:
    def __init__(self):
        
        self.config = Configuration(
            service_endpoint=EXCHANGE_SERVER, retry_policy=FaultTolerance(max_wait=3600), credentials=Credentials(USERNAME, PASSWORD))
        self.account = Account(ANALYTICS_EMAIL_ADDRESS, config=self.config, access_type=DELEGATE)

    # datetime.datetime -> exchangelib.EWSDate
    # Consumes a date and produces a localized start date
    def get_start_date(self, date):
        return self.account.default_timezone.localize(EWSDateTime(date.year, date.month, date.day))

    # String datetime.datetime -> List of exchangelib.Message
    # Consumes an email subject and a start date and produces a list of emails not longer than 100
    def get_emails(self, email_subject, date):
        start = self.get_start_date(date)
        return self.account.inbox.filter(subject__contains=email_subject).filter(datetime_received__gt=start).order_by('-datetime_received')[:100]

    # List of exchangelib.Message String -> None
    # Consumes a list of emails and saves attachments
    def save_email_attachments(self, emails, file_path, attachment_directories):

        if(len(emails) == 0):
            return None
        else:
            first = emails[0]
            rest = emails[1:]

            self.save_attachments(first.attachments, file_path, first.datetime_received, attachment_directories)
            
            self.save_email_attachments(rest, file_path, attachment_directories)

    # List of exchangelib.FileAttachment String Date -> None
    # Consumes a list of attachments and saves each one to file_path
    def save_attachments(self, attachments, file_path, date_received, attachment_directories):
        for attachment in attachments:
            if isinstance(attachment, FileAttachment):
                self.save_attachment(attachment, file_path, date_received, attachment_directories)

    # exchangelib.FileAttachment String Date -> None
    # Consumes an attachment and saves it to file_path with the date stamp inserted into the filename
    def save_attachment(self, attachment, file_path, date_received, attachment_directories):  
        full_file_path = self.get_file_path(file_path, attachment.name, date_received, attachment_directories)
        
        if (full_file_path is None):
            error_email(__name__, 'save_attachment',\
             f'The list of directories supplied does not match the email attachment name: {attachment.name}')
        else:
            with open(full_file_path, 'wb') as f:
                f.write(attachment.content)

    def get_file_path(self, file_path, attachment_name, date_received, attachment_directories):
        full_file_path = None

        if(len(attachment_directories) == 0):
            full_file_path = insert_datestamp_in_filename(file_path, attachment_name, date_received)
        else:
            for d in attachment_directories:
                if d.lower() in attachment_name.lower():  
                    full_file_path = insert_datestamp_in_filename(os.path.join(file_path, d), attachment_name, date_received)
        
        return full_file_path