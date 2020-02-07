from exchangelib import DELEGATE, NTLM, Configuration, Credentials, Account, FileAttachment, EWSDateTime
import os
import time
import re
import datetime
import sys
sys.path.append('..')
from services.exchangeWrapper import ExchangeWrapper
from helpers.log import log as log
from helpers.dataAccess import DataAccess
from helpers.utils import get_date_from_string, load_json, write_to_csv, files_later_than
from services.excelExtractor import extract_files
import pyodbc

from decimal import Decimal
from cred_secrets import USERNAME, PASSWORD
from constants import EXCHANGE_SERVER, EMAIL_ADDRESS
import pandas as pd
import csv


class EmailImporter(object):
    def __init__(self, email_subject, file_path, database_server, database, name, table_name, file_parts):
        self.email_subject = email_subject
        self.file_path = file_path
        self.name = name   
        self.table_name = table_name 
        self.database_server = database_server
        self.database = database    
        self.file_parts = file_parts     


    def run(self):
        try:
            data_access = DataAccess(self.database_server, self.database)  
            file_list = os.listdir(self.file_path)

            exchangeWrapper = ExchangeWrapper()
            
            max_database_date = datetime.datetime.now()
            for part in self.file_parts:
                d = {'Curve':[part]}

                max_database_date = min(max_database_date, data_access.get_max_database_date(self.table_name, 'Asof', schema_name='import', **d))
                
                list_emails = exchangeWrapper.get_emails(self.email_subject, max_database_date)
                exchangeWrapper.save_email_attachments(list(list_emails), self.file_path)
                time.sleep(1)
            log(__name__, '__run__', f"{self.name} has downloaded all emails with the subject: {self.email_subject}", level="Info")
        except Exception as e:
            log(__name__, '__run__', f"{self.name} has failed: {str(e)}", level="Error", email=True, emailSubject=self.name)

        
        #filtered_files = files_later_than(self.file_list, self.max_database_date)
        #print(self.name.replace(' ', '_'))
        #list_dicts = extract_files(filtered_files, )
        #print(list_dicts)
        # write_to_csv(list_dicts, self.file_path)

        # log(__name__, 'run', f"Complete")




if(__name__ == "__main__"):   

    emailImporter = EmailImporter(
        'ICE 1630 SGT Futures',
        "{}\\ICE_Settlement\\Attachments".format(os.environ['ADUB_Import_Path']),
        database_server=os.environ['ADUB_DBServer'],
        database='Price',
        name='ICE 1630 SGT Futures',
        table_name = 'ICE_Settlement_Curve'

    )
    emailImporter.run()