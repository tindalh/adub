from exchangelib import DELEGATE, NTLM, Configuration, Credentials, Account, FileAttachment, EWSDateTime
import os
import re
import datetime
import sys
sys.path.append('..')
from helpers.log import log as log
import pyodbc
import json
import xlrd
from decimal import Decimal
from cred_secrets import USERNAME, PASSWORD
from constants import EXCHANGE_SERVER, ANALYTICS_EMAIL_ADDRESS
import pandas as pd
import csv


class McQuilling(object):
    def __init__(self, email_subject, file_path, database_server, database):
        self.email_subject = email_subject
        self.file_path = file_path
        self.database_server = database_server
        self.database = database

    def run(self):

        if(not os.path.isdir(self.file_path)):
            log(__name__, 'run',
                f"Path does not exist {self.file_path}", level="Error")
            return

        config = Configuration(
            service_endpoint=EXCHANGE_SERVER, credentials=Credentials(USERNAME, PASSWORD))
        account = Account(ANALYTICS_EMAIL_ADDRESS, config=config, access_type=DELEGATE)

        max_saved = self.get_max_file_date(os.listdir(self.file_path))

        log(__name__, 'run', "Max file name saved: " + str(max_saved))

        try:
            list_emails = list(self.get_emails(datetime.datetime.strptime(
                str(max_saved), '%Y%m%d'), 'Daily Freight Rate Assessment', account))

            log(__name__, 'run', "Saving attachments in " + os.path.join(self.file_path, "Attachments"))

            self.get_attachments(list_emails, os.path.join(self.file_path, "Attachments"))
        except Exception as e:
            log(__name__, 'run', "Error: " + str(e), level="Error")
        
        log(__name__, 'run', "Saving files to " + "{}\\McQuilling\\McQuilling_{}.csv".format(os.environ['ADUB_Import_Path'], datetime.date.strftime(datetime.date.today(), "%Y%m%d")))
        
        self.write_to_csv(self.extract_mcquilling(), "{}\\McQuilling\\McQuilling_{}.csv".format(os.environ['ADUB_Import_Path'], datetime.date.strftime(datetime.date.today(), "%Y%m%d")))

        log(__name__, 'run', f"Complete")

    def get_max_file_date(self, file_list):

        base_case = 20000101

        if(len(file_list) == 0):
            return base_case
        else:
            try:
                # Expect 'Daily Freight Rate Assessment_2019_20191004.xlsm' -> '20191004'
                first = int(file_list[0].split('.')[-2:-1][0][-8:])
            except:  # Ignoring invalid file name
                first = base_case

            rest = file_list[1:]
            max_rest = self.get_max_file_date(rest)

            if(first >= max_rest):
                return first
            else:
                return max_rest

    def get_start_date(self, max_existing, account):
        return account.default_timezone.localize(EWSDateTime(max_existing.year, max_existing.month, max_existing.day))

    def get_emails(self, max_existing, email_subject, account):
        start = self.get_start_date(max_existing, account)

        return account.inbox.filter(subject__contains=email_subject).filter(datetime_received__gt=start).order_by('-datetime_received')[:100]

    # ListOfEmails -> None
    # Consumes a list of emails and saves attachments
    def get_attachments(self, list_emails, file_path):

        if(len(list_emails) == 0):
            return None
        else:
            first = list_emails[0]
            rest = list_emails[1:]

            for attachment in first.attachments:
                if isinstance(attachment, FileAttachment):

                    full_file_path = os.path.join(
                        file_path, f"{attachment.name.split('.')[0]}_{first.datetime_received.strftime('%Y%m%d')}.xlsm")

                    with open(full_file_path, 'wb') as f:
                        f.write(attachment.content)
                        print('Saved attachment to', full_file_path)

            self.get_attachments(rest, file_path)


    def extract_mcquilling(self):

        cnxn = pyodbc.connect(
            f"driver=SQL Server;server={self.database_server};database={self.database};trusted_connnection=yes")
        cursor = cnxn.cursor()

        max_date_string = cnxn.execute(
            "SELECT Max(DateStamp) FROM  [import].[McQuilling]").fetchval()

        try:
            max_date = datetime.datetime.strptime(max_date_string, '%Y-%m-%d')
        except:
            max_date = datetime.datetime.strptime('2018-01-01', '%Y-%m-%d')

        file_list = os.listdir(os.path.join(self.file_path,"Attachments"))
        log(__name__, 'run', "Extracting from " + os.path.join(self.file_path,"Attachments"))

        list_dicts = []

        try:

            for filename in file_list:

                file_name = os.path.join(os.path.join(self.file_path,"Attachments"), filename)

                try:
                    datestamp = datetime.datetime.strptime(
                        str(int(file_name.split('.')[-2:-1][0][-8:])), '%Y%m%d')
                except:
                    datestamp = datetime.datetime.strptime(str(20000101), '%Y%m%d')

                if datestamp > max_date:
                    wb = xlrd.open_workbook(file_name)
                    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)),'mcQuilling.json')) as json_file:
                        sheet_json = json.load(json_file)

                    sheet = wb.sheet_by_name(sheet_json["name"])

                    

                    for section in sheet_json["columns"]:
                        for block in section["blocks"]:
                            
                            for y in range(0, sheet.nrows):
                                if(str(sheet.cell_value(y,section["start"])).lower().strip() == str(block["name"]).lower().strip() ):
                                
                                    for row in range(y+2, sheet.nrows):
                                        if(len(str(sheet.cell_value(row, section["start"]))) == 0 or "*" == sheet.cell_value(row, section["start"])[0]):
                                            break
                                        
                                        row_dict = {}
                                        # TODO - these two only work for McQuilling
                                        row_dict["DateStamp"] = datetime.date.strftime( datestamp, "%Y-%m-%d")
                                        row_dict["Block"] = block["name"]
                                        for config_column in block["columns"]:
                                            found = False
                                            for x in range(section["start"], section["end"] - section["start"] + section["start"]):
                                                    if(sheet.cell_value(y+1, x) == config_column["theirs"]):
                                                        if(sheet.cell_type(row, x) == 1):
                                                            row_dict[config_column["ours"]] = re.sub("([0-9])(?:m)", r'\1', sheet.cell_value(row, x).replace('*',''))  # TODO - this only works for McQuilling
                                                        else:
                                                            row_dict[config_column["ours"]] = sheet.cell_value(row, x)
                                                        found = True
                                            if (not found):
                                                if(config_column.get("value", "") == ""):
                                                    row_dict[config_column["ours"]] = ""
                                                else: 
                                                    row_dict[config_column["ours"]] = config_column["value"]

                                        list_dicts.append(row_dict)
            cnxn.close()     

        except Exception as e:
            log(__name__, 'run', "Error: " + str(e), level="Error")
        return list_dicts
        

    def write_to_csv(self, list_of_dicts, file_path):
        if(len(list_of_dicts) == 0):
            return

        keys = list_of_dicts[0].keys()
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, keys, delimiter='|')
            writer.writeheader()
            writer.writerows(list_of_dicts)


if(__name__ == "__main__"):
    mcQ = McQuilling(
        'Daily Freight Rate Assessment',
        "{}\\McQuilling\\Attachments".format(os.environ['ADUB_Import_Path']),
        database_server=os.environ['ADUB_DBServer'],
        database='Price'
    )
    mcQ.run(USERNAME, PASSWORD, EXCHANGE_SERVER, ANALYTICS_EMAIL_ADDRESS)