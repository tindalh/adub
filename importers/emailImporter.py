from exchangelib import DELEGATE, NTLM, Configuration, Credentials, Account, FileAttachment, EWSDateTime
import os
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
    def __init__(self, email_subject, file_path, database_server, database, name, table_name):
        self.email_subject = email_subject
        self.file_path = file_path
        self.name = name   
        self.table_name = table_name  

        
        


    def run(self):

        data_access = DataAccess(database_server, database)  
        file_list = os.listdir(self.file_path)
        save_to_path = self.data_access.server + "\\" + self.name + "\\" + self.name + "_" + datetime.date.strftime(datetime.date.today(), "%Y%m%d") + ".csv"
        max_database_date = data_access.get_max_database_date(self.table_name, 'Asof')
        list_emails = self.exchangeWrapper.get_emails(self.email_subject, self.max_database_date)
        exchangeWrapper = ExchangeWrapper()
        exchangeWrapper.save_email_attachments(list(list_emails), self.file_path)
        
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