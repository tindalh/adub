from exchangelib import FileAttachment, Configuration, Credentials, Account, DELEGATE, EWSDateTime
import datetime
import sys
sys.path.append('..')
from helpers.utils import insert_datestamp_in_filename

from cred_secrets import USERNAME, PASSWORD
from constants import ANALYTICS_EMAIL_ADDRESS, EXCHANGE_SERVER

class ExchangeWrapper:
    def __init__(self):
        self.config = Configuration(
            service_endpoint=EXCHANGE_SERVER, credentials=Credentials(USERNAME, PASSWORD))
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
    def save_email_attachments(self, emails, file_path):

        if(len(emails) == 0):
            return None
        else:
            first = emails[0]
            rest = emails[1:]

            self.save_attachments(first.attachments, file_path, first.datetime_received)

            self.save_email_attachments(rest, file_path)

    # List of exchangelib.FileAttachment String Date -> None
    # Consumes a list of attachments and saves each one to file_path
    def save_attachments(self, attachments, file_path, date_received):
        for attachment in attachments:
            if isinstance(attachment, FileAttachment):
                self.save_attachment(attachment, file_path, date_received)

    # exchangelib.FileAttachment String Date -> None
    # Consumes an attachment and saves it to file_path with the date stamp inserted into the filename
    def save_attachment(self, attachment, file_path, date_received):    
        full_file_path = insert_datestamp_in_filename(file_path, attachment.name, date_received)

        with open(full_file_path, 'wb') as f:
            f.write(attachment.content)