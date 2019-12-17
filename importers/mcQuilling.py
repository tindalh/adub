from exchangelib import DELEGATE,NTLM,Configuration, Credentials, Account, FileAttachment, EWSDateTime
import os
import datetime
from helpers.log import log as log
import pyodbc
import xlrd
from decimal import Decimal

class McQuilling(object):
    def __init__(self, email_subject, file_path, database_server, database):
        self.email_subject = email_subject
        self.file_path = file_path
        self.database_server = database_server
        self.database = database

    def run(self, user, password, exchange_server, email_address):

        if(not os.path.isdir(self.file_path)):
            log(__name__, 'run', f"Path does not exist {self.file_path}", level="Error")
            return

        
        config = Configuration(service_endpoint=exchange_server, credentials=Credentials(user, password))
        account = Account(email_address, config=config, access_type=DELEGATE)

        max_saved = self.get_max_file_date(os.listdir(self.file_path))

        log(__name__, 'run', "Max file name saved: " + str(max_saved))
        list_emails = list(self.get_emails(datetime.datetime.strptime(str(max_saved), '%Y%m%d'), 'Daily Freight Rate Assessment', account))
        self.get_attachments(list_emails, self.file_path)
        self.extract_mcquilling()

        log(__name__, 'run', f"Complete")

    def get_max_file_date(self, file_list):
        
        base_case = 20000101

        if(len(file_list) == 0):            
            return base_case
        else:
            try:
                first = int(file_list[0].split('.')[-2:-1][0][-8:]) # Expect 'Daily Freight Rate Assessment_2019_20191004.xlsm' -> '20191004'
            except: # Ignoring invalid file name                
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
        
        return account.inbox.filter(subject__contains=email_subject ).filter(datetime_received__gt=start).order_by('-datetime_received')[:100]

    # ListOfEmails -> None
    # Consumes a list of emails and saves attachments
    def get_attachments(self, list_emails, file_path):
        
        if(len(list_emails) == 0):            
            return None
        else:
            first = list_emails[0]
            rest =  list_emails[1:]  

            for attachment in first.attachments:
                if isinstance(attachment, FileAttachment):

                    full_file_path = os.path.join(file_path, f"{attachment.name.split('.')[0]}_{first.datetime_received.strftime('%Y%m%d')}.xlsm")

                    with open(full_file_path, 'wb') as f:
                        f.write(attachment.content)
                        print('Saved attachment to', full_file_path)

            self.get_attachments(rest, file_path)


    def save_excel_table(self, cnxn, wb, sheet, r_start, r_end, c_start, datestamp, vClass, voytype, file_name, cursor ):
        try:
            for r in range(r_start,r_end):
                if voytype == "Roundtrip":
                    if str(sheet.cell_value(r, c_start))[-1] in ('*'):
                        voyage = str(sheet.cell_value(r, c_start))[:-1]
                    else:
                        voyage = str(sheet.cell_value(r, c_start))
                    voyage = sheet.cell_value(r, c_start)
                    tons = sheet.cell_value(r, c_start+1)
                    if str(sheet.cell_value(r, c_start+2))[-1] in ("m", "M") :
                        ws = str(sheet.cell_value(r, c_start+2))[:-1]
                    else:
                        ws = sheet.cell_value(r, c_start+2)
                    tce = sheet.cell_value(r, c_start+3)
                    demmurage = sheet.cell_value(r, c_start+4)
                    comments = sheet.cell_value(r, c_start+5)
                else:
                    if str(sheet.cell_value(r, c_start))[-1] in ('*'):
                        voyage = str(sheet.cell_value(r, c_start))[:-1]
                    else:
                        voyage = str(sheet.cell_value(r, c_start))
                    tons = sheet.cell_value(r, c_start+1)
                    if str(sheet.cell_value(r, c_start+2))[-1] in ("m", "M") :
                        ws = str(sheet.cell_value(r, c_start+2))[:-1]
                    else:
                        ws = sheet.cell_value(r, c_start+2)
                    tce = ""
                    demmurage = sheet.cell_value(r, c_start+3)
                    comments = sheet.cell_value(r, c_start+4)

                if voyage == 'SVOE/W''SHAVEN':
                    voyage = 'UKC/UKC'
                elif voyage == 'V''COUVER/USWC':
                    voyage = 'VANCOUVER/USWC'
                elif voyage == 'V''COUVER/USG':
                    voyage = 'VANCOUVER/USG'
                elif voyage == 'ROTTERDAM/SPORE':
                    voyage = 'NSEA/SPORE'
                elif voyage == 'CARIBS/USG' and vClass == 'Panamax':
                    voyage = 'CARIBS/USG-AC'
                elif voyage == 'ARZEW/NINGBO':
                    voyage = 'MED/CHINA'
                if vClass == 'Panamazx':
                    vClass = 'Panamax'
                
                cursor.execute ("""INSERT INTO  [import].[McQuilling] (DateStamp, Class, Voyage, Tons, WS, TCE, Demurrage, Comments, VoyageType, IsDirty) VALUES (?, ? , ? , ? , ?, ?, ?, ? , ?, ?)""",\
                (datestamp, vClass, voyage, tons, ws, tce, demmurage, comments, voytype, 1 ))
                cnxn.commit()
        except Exception as e:
            log(__name__, 'save_excel_table', f"Error processing file {file_name}", 'Error')

    def extract_mcquilling(self):

        cnxn = pyodbc.connect(f"driver=SQL Server;server={self.database_server};database={self.database};trusted_connnection=yes")
        cursor = cnxn.cursor()

        max_date_string = cnxn.execute ("SELECT Max(DateStamp) FROM  [import].[McQuilling]").fetchval()

        try : 
            max_date = datetime.datetime.strptime(max_date_string, '%Y-%m-%d')
        except :
            max_date = datetime.datetime.strptime('2018-01-01', '%Y-%m-%d')

        file_list = os.listdir(self.file_path)

        for filename in file_list:
            
            file_name = os.path.join(self.file_path, filename)

            try:
                datestamp =  datetime.datetime.strptime(str(int(file_name.split('.')[-2:-1][0][-8:])), '%Y%m%d')
            except:
                datestamp = datetime.datetime.strptime(str(20000101), '%Y%m%d') 
            
            if datestamp > max_date:

                wb = xlrd.open_workbook(file_name) 
                sheet = wb.sheet_by_index(0) 
                
                self.save_excel_table(cnxn, wb, sheet, 9, 21, 0,  datestamp, "VLCC", "Roundtrip", file_name, cursor)
                self.save_excel_table(cnxn, wb, sheet, 25, 35, 0,  datestamp, "Suezmax", "Roundtrip", file_name, cursor)
                self.save_excel_table(cnxn, wb, sheet, 39, 47, 0,  datestamp, "Aframax", "Roundtrip", file_name, cursor)
                self.save_excel_table(cnxn, wb, sheet, 51, 54, 0,  datestamp, "Panamazx", "Roundtrip", file_name, cursor)
                self.save_excel_table(cnxn, wb, sheet, 9, 12, 7, datestamp, "VLCC", "Non-Roundtrip", file_name, cursor)
                self.save_excel_table(cnxn, wb, sheet, 25, 29, 7,  datestamp, "Suezmax", "Non-Roundtrip", file_name, cursor)
                self.save_excel_table(cnxn, wb, sheet, 39, 45, 7,  datestamp, "Aframax", "Non-Roundtrip", file_name, cursor)
                self.save_excel_table(cnxn, wb, sheet, 51, 57, 7,  datestamp, "Panamax", "Non-Roundtrip", file_name, cursor)
                self.save_excel_table(cnxn, wb, sheet, 63, 66, 7,  datestamp, "VLCC", "Triangulation", file_name, cursor)

                wb.release_resources()
                
                log(__name__, 'run', f"Saved {file_name}")
                
        cnxn.close()


    

            
                    