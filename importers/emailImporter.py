from exchangelib import DELEGATE, NTLM, Configuration, Credentials, Account, FileAttachment, EWSDateTime
import os
import time
import re
import datetime
import sys
sys.path.append('..')
from services.exchange import account, save_email_attachments, get_emails
from helpers.log import log as log
from helpers.dataAccess import DataAccess
from helpers.utils import get_date_from_string, load_json, write_to_csv, files_later_than
from services.excelExtractor import extract_files
import pyodbc
from os import walk

from decimal import Decimal
import pandas as pd
import csv


class EmailImporter(object):
    def __init__(self, email_subject, file_path, database_server, database, name, table_name, file_parts, fn_get_max_saved, fn_save=None):
        self.email_subject = email_subject
        self.file_path = file_path
        self.name = name   
        self.table_name = table_name 
        self.database_server = database_server
        self.database = database    
        self.file_parts = file_parts
        self.fn_get_max_saved = fn_get_max_saved    
        self.fn_save = fn_save 


    def run(self):        
        try:
            data_access = DataAccess(self.database_server, self.database)
            list_emails = get_emails(account(),
                self.email_subject, \
                    self.fn_get_max_saved(data_access, self.table_name, self.file_parts))

            save_email_attachments(list(list_emails), self.file_path, self.file_parts)

            if (self.fn_save is not None):
                self.fn_save(self.file_parts)

            log(__name__, '__run__', f"{self.name} has downloaded all emails with the subject: {self.email_subject}", level="Info")
        except Exception as e:
            log(__name__, '__run__', f"{self.name} has failed: {str(e)}", level="Error", email=True, emailSubject=self.name)
            raise e

      