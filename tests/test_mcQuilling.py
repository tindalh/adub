
import sys
import os
sys.path.append('..')
from importers.mcQuilling import McQuilling
from cred_secrets import USERNAME, PASSWORD
import unittest
from unittest import mock
import datetime
from exchangelib import DELEGATE,Configuration, Credentials, Account, FileAttachment, EWSDateTime

EXCHANGE_SERVER = 'https://loneca.arcpet.co.uk/EWS/Exchange.asmx'
EMAIL_ADDRESS = 'henryt@arcpet.co.uk'

class TestMcQuilling(unittest.TestCase):
    def setUp(self):
        
        self.mcQuilling = McQuilling(
            'Daily Freight Rate Assessment',
            "{}\\McQuilling".format(os.environ['ADUB_Import_Path']),
            database_server='Lon-Pc53',
            database='Price'
        )

        self.file_list = [
            'Daily Freight Rate Assessment_2019_20191113.xlsm',
            'Daily Freight Rate Assessment_2019_20191201.xlsm'
        ]
        self.bad_file_list = [
            'Daily Freight Rate Assessment_2019.xlsm',
            'Daily Freight Rate Assessment_2019_20191201.xlsm'
        ]

    # python -m unittest test_mcQuilling.TestMcQuilling.test_get_attachments
    def test_get_attachments(self):
        self.assertIsNone(
            self.mcQuilling.get_attachments(
                list(self.mcQuilling.get_emails(datetime.datetime.strptime(str(20191213), '%Y%m%d'), 'Daily Freight Rate Assessment')), self.mcQuilling.file_path))  

    # python -m unittest test_mcQuilling.TestMcQuilling.test_run
    def test_run(self):
        self.assertIsNone(self.mcQuilling.run(USERNAME, PASSWORD, EXCHANGE_SERVER, EMAIL_ADDRESS))

    # python -m unittest test_mcQuilling.TestMcQuilling.test_run_with_bad_path
    def test_run_with_bad_path(self):
        temp_path = self.mcQuilling.file_path
        self.mcQuilling.file_path = "bad_path"
        self.assertIsNone(self.mcQuilling.run(USERNAME, PASSWORD, EXCHANGE_SERVER, EMAIL_ADDRESS))
        self.mcQuilling.file_path = temp_path

    # python -m unittest test_mcQuilling.TestMcQuilling.test_get_max_file_date
    def test_get_max_file_date(self):
        self.assertEqual(self.mcQuilling.get_max_file_date(self.file_list), 20191201)

    # python -m unittest test_mcQuilling.TestMcQuilling.test_get_max_file_date_with_bad_file_name
    def test_get_max_file_date_with_bad_file_name(self):
        self.assertEqual(self.mcQuilling.get_max_file_date(self.bad_file_list), 20191201)
