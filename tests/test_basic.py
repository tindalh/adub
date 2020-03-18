from io import StringIO
import collections
import unittest
import time
import pyodbc
import json
import pandas as pd
import numpy
import sys
import os
from datetime import datetime, date
import xlrd
from exchangelib import DELEGATE, NTLM, Configuration, Credentials, Account, FileAttachment, EWSDateTime, Mailbox, Message
from unittest.mock import MagicMock, Mock, patch
from unittest import mock
from multiprocessing import Process, Queue


sys.path.append('..')
import helpers.dataAccess as dtAccss
import helpers.csvHelper as csvHlpr
import importers.clipperDataImporter as clpprDtaImprtr
import integrators.csvIntegrator as csvIntgrtr
from cleaners.clipperFloatingStorageCleaner import __arrangeClipperColumns__
from cleaners.ieaTxtCleaner import clean as cleanIeaTxt
from cleaners.ieaTxtCleaner import _replaceInvalidQuantities, _cleanDateColumns, _setFrequency, _remove_header_row, clean as clean_iea
from cleaners.rystadCleaner import clean as rystadCleaner
from cleaners.clipperFloatingStorageCleaner import clean as clipperFloatingStorageCleaner
from cleaners.ice_settlementCleaner import clean as ice_cleaner
from cleaners.ice_settlementCleaner import clean, _set_asof, _format_contract_column, \
    _parse_contract_column, _set_curve_name_column, _arrange_columns
from cleaners.rystadCleaner import clean, __setWeightedSulphur__, __arrangeColumns__, __getWeightedSulphur__
from helpers.csvHelper import getDataframe, extract_dataframe
from helpers.dataAccess import DataAccess
from helpers.log import log as old_log
import helpers.log as log
from helpers.utils import get_date_from_string, insert_datestamp_in_filename, \
    load_json, get_max_file_date, write_to_csv, files_later_than, \
    get_unique_values_for_dataframe_keys
import importers.eiaImporter as eia
from importers.emailImporter import EmailImporter
from services.excelExtractor import extract_files, _extract_sections, \
    _extract_blocks, _extract_rows, _extract_columns, _get_block_start, \
     _get_block_end, _get_value
from services.exchangeWrapper import ExchangeWrapper
from services.priceReturns import *
from exchangelib import DELEGATE,Configuration, Credentials, Account, FileAttachment, EWSDateTime
from constants import ANALYTICS_EMAIL_ADDRESS, EXCHANGE_SERVER
from service_constants import *
from importers.mcQuilling import McQuilling
from cred_secrets import USERNAME, PASSWORD


def get_json_result(url):
        return json.dumps("")

class ClipperDataImporterCase(unittest.TestCase):
    # str -> json
    # Consumes a url and produces the resulting json

    # python -m unittest test_clipperDataImporter.ClipperDataImporterCase.test_get_json_result
    def test_get_json_result(self):
        self.assertEqual(get_json_result(""), json.dumps(""))
    
    # python -m unittest test_clipperDataImporter.ClipperDataImporterCase.test_import

    

    def getDataFrame(self, response):
        data = json.loads(response)['record']
        df = pd.DataFrame.from_dict(data)
        return df

    def flattenColumns(self, df):
        df = pd.concat([df.drop(['measuresGlobalCrudeEntityPK'], axis=1), df['measuresGlobalCrudeEntityPK'].apply(pd.Series)], axis=1)
        return df

    def deleteUpdatedOrRemoved(self, ids_to_delete, dataAccess, table_name, id_name):
        dataAccess.deleteById(table_name, id_name, ids_to_delete)

    def arrangeColumns(self, df, dataAccess, table_name):
        table_columns = dataAccess.load("view_TableColumns", TableName= table_name)

        series = list()
        for columnInfo in table_columns:
            for df_column in df.columns:
                if(columnInfo.ColumnName.lower() == df_column.lower()):
                    series.append(df[df_column])

        dfResult = pd.concat(series, axis=1)

        return dfResult

    def test_import(self):
        df = self.getDataFrame(self.response)
        
        df = self.flattenColumns(df)
        
        dataAccess = dtAccss.DataAccess('Lon-PC53', 'STG_Targo')

        ids_to_delete = ','.join([str(x) for  x in df['rowNum'].unique()])
        
        self.deleteUpdatedOrRemoved(ids_to_delete, dataAccess, self.clipperDataImporter.table_name, 'rowNum')

        dfResult = self.arrangeColumns(df, dataAccess, self.clipperDataImporter.table_name)

        dfResult.to_csv(self.clipperDataImporter.output_file_path, sep='|', index=False)

        dataAccess.bulkInsert(self.clipperDataImporter.table_name, self.clipperDataImporter.output_file_path)


    def setUp(self):
        self.clipperDataImporter = clpprDtaImprtr.ClipperDataImporter(
            url="test",
            user = "targo.support@arcpet.co.uk",
            server= "Lon-PC53",
            database = "STG_Targo",
            password = "arcsupport212",
            output_file_path="{}\\ClipperData\\clipperData.csv".format(os.environ['ADUB_Import_Output_UNC']),
            table_name='yview_ClipperStaging'
        )
        self.response="""
            {
            "request_status":"VALID_REQUEST",
            "record":[
                {
                    "type":"measuresGlobalCrudeEntity",
                    "loadArea":"KOR",
                    "offtakeArea":"CH-S",
                    "offtakePoint":"",
                    "api":28.0,
                    "area":"",
                    "bbls":47629,
                    "bblsNominal":420000,
                    "bill":"",
                    "charter_grade":"",
                    "charter_load_area":"",
                    "charter_offtake_area":"",
                    "charterer":"",
                    "consignee":"",
                    "declaredDest":"MY TANJUNG PELEPAS",
                    "domExp":"",
                    "grade":"CRUDE",
                    "gradeApi":"medium",
                    "gradeCountry":"UNKNOWN",
                    "gradeRegion":"UNKNOWN",
                    "gradeSulfur":"sweet",
                    "imo":9289489,
                    "lightering_vessel":"",
                    "loadAreaDescr":"Korea",
                    "loadCountry":"SOUTH KOREA",
                    "loadDate":"2017-07-23T00:00:00Z",
                    "loadOwner":"KNOC",
                    "loadPoint":"KNOC - Yeosu Sapo Terminal",
                    "loadPort":"YEOSU",
                    "loadPortBill":"",
                    "loadRegion":"EAST ASIA",
                    "loadState":"",
                    "loadStsVessel":"",
                    "load_sts_imo":"",
                    "measuresGlobalCrudeEntityPK":{
                        "dateNum":598606,
                        "rowNum":359766,
                        "statNum":0
                    },
                    "offtakeAreaDescr":"China South",
                    "offtakeCountry":"",
                    "offtakeDate":"2017-08-01T00:00:00Z",
                    "offtakeOwner":"",
                    "offtakePort":"",
                    "offtakePortBill":"",
                    "offtakeRegion":"EAST ASIA",
                    "offtakeState":"",
                    "offtakeStsVessel":"",
                    "offtake_sts_imo":"",
                    "opecNopec":"",
                    "probability":0.1134021,
                    "probabilityGroup":"0-25",
                    "projection":"Yes",
                    "route":"",
                    "shipper":"",
                    "source":"AIS",
                    "storage":"No",
                    "storageZone":"",
                    "subArea":"",
                    "sulfur":0.2,
                    "vessel":"Csk Shelton",
                    "vesselClass":"Aframax",
                    "vesselFlag":"HONG KONG"
                },
                {
                    "type":"measuresGlobalCrudeEntity",
                    "loadArea":"AG",
                    "offtakeArea":"JAP",
                    "offtakePoint":"JXTG - Kashima Refinery",
                    "api":39.2,
                    "area":"",
                    "bbls":346000,
                    "bblsNominal":346000,
                    "bill":"",
                    "charter_grade":"",
                    "charter_load_area":"",
                    "charter_offtake_area":"",
                    "charterer":"",
                    "consignee":"",
                    "declaredDest":"",
                    "domExp":"",
                    "grade":"DAS",
                    "gradeApi":"light",
                    "gradeCountry":"UNITED ARAB EMIRATES",
                    "gradeRegion":"ARAB GULF",
                    "gradeSulfur":"sour",
                    "imo":9478664,
                    "lightering_vessel":"",
                    "loadAreaDescr":"Arab Gulf",
                    "loadCountry":"UNITED ARAB EMIRATES",
                    "loadDate":"2017-04-09T00:00:00Z",
                    "loadOwner":"Adnoc",
                    "loadPoint":"ADNOC - Das Island SBM",
                    "loadPort":"DAS",
                    "loadPortBill":"",
                    "loadRegion":"ARAB GULF",
                    "loadState":"",
                    "loadStsVessel":"",
                    "load_sts_imo":"",
                    "measuresGlobalCrudeEntityPK":{
                        "dateNum":598607,
                        "rowNum":301301,
                        "statNum":0
                    },
                    "offtakeAreaDescr":"Japan",
                    "offtakeCountry":"JAPAN",
                    "offtakeDate":"2017-05-01T00:00:00Z",
                    "offtakeOwner":"JXTG Holdings Inc.",
                    "offtakePort":"KASHIMA KO",
                    "offtakePortBill":"",
                    "offtakeRegion":"EAST ASIA",
                    "offtakeState":"",
                    "offtakeStsVessel":"",
                    "offtake_sts_imo":"",
                    "opecNopec":"Export OPEC",
                    "probability":1.0,
                    "probabilityGroup":"75-100",
                    "projection":"No",
                    "route":"",
                    "shipper":"",
                    "source":"AIS",
                    "storage":"No",
                    "storageZone":"",
                    "subArea":"",
                    "sulfur":1.3,
                    "vessel":"Takaoka",
                    "vesselClass":"VLCC",
                    "vesselFlag":"JAPAN"
                },
                {
                    "type":"measuresGlobalCrudeEntity",
                    "loadArea":"AG",
                    "offtakeArea":"CH-C",
                    "offtakePoint":"",
                    "api":18.1,
                    "area":"",
                    "bbls":521111,
                    "bblsNominal":837500,
                    "bill":"",
                    "charter_grade":"",
                    "charter_load_area":"",
                    "charter_offtake_area":"",
                    "charterer":"",
                    "consignee":"",
                    "declaredDest":"",
                    "domExp":"",
                    "grade":"SOROOSH (CYRUS)",
                    "gradeApi":"heavy",
                    "gradeCountry":"IRAN",
                    "gradeRegion":"ARAB GULF",
                    "gradeSulfur":"sour",
                    "imo":9218492,
                    "lightering_vessel":"",
                    "loadAreaDescr":"Arab Gulf",
                    "loadCountry":"IRAN",
                    "loadDate":"2017-07-21T00:00:00Z",
                    "loadOwner":"NIOC",
                    "loadPoint":"NIOC - Khalij E Fars FSO",
                    "loadPort":"KHARG ISLAND",
                    "loadPortBill":"",
                    "loadRegion":"ARAB GULF",
                    "loadState":"",
                    "loadStsVessel":"",
                    "load_sts_imo":"",
                    "measuresGlobalCrudeEntityPK":{
                        "dateNum":598608,
                        "rowNum":301740,
                        "statNum":0
                    },
                    "offtakeAreaDescr":"China Central",
                    "offtakeCountry":"",
                    "offtakeDate":"2017-08-14T00:00:00Z",
                    "offtakeOwner":"",
                    "offtakePort":"",
                    "offtakePortBill":"",
                    "offtakeRegion":"EAST ASIA",
                    "offtakeState":"",
                    "offtakeStsVessel":"",
                    "offtake_sts_imo":"",
                    "opecNopec":"Export OPEC",
                    "probability":0.6222222,
                    "probabilityGroup":"50-75",
                    "projection":"Yes",
                    "route":"",
                    "shipper":"",
                    "source":"AIS",
                    "storage":"No",
                    "storageZone":"",
                    "subArea":"",
                    "sulfur":3.3,
                    "vessel":"Deep Sea",
                    "vesselClass":"VLCC",
                    "vesselFlag":"PANAMA"
                }]}
        """


class TestClipperFloatingStorageCleaner(unittest.TestCase):
    # python -m unittest test_clipperFloatingStorageCleaner.ClipperFloatingStorageCleanerCase.test__arrangeClipperColumns__
    def test__arrangeClipperColumns__(self):
        mystr = """
            01-01-2017
            01-01-2019
        """
        columns = ['date_asof']
        
        df = pd.read_csv(StringIO(mystr), header=None, sep='|', names=columns)

        self.assertEqual(__arrangeClipperColumns__(df, '2018-01-01').iloc[0].tolist(), ['2019-01-01'])

class CsvHelperCase(unittest.TestCase):

    # python -m unittest test_csvHelper.CsvHelperCase.test_getDataFrame
    def test_getDataFrame(self):
        self.assertEqual(getDataframe("data\multi_space_separated.txt", '\s+').iloc[0].tolist(),['AUSTRALIA', 'CRUDE', 'FEB2005', 390.358])

        self.assertEqual(getDataframe("data\multi_space_separated.txt", '\s+', names=['a','b','c','d']).columns.tolist() ,['a','b','c','d'])

        self.assertEqual(getDataframe("data\comma_separated.csv", ',').iloc[0].tolist(),[2013,'January','Aasgard Blend'])

        self.assertEqual(getDataframe("data\\tab_separated.csv", '\t').iloc[0].tolist(),['01/01/2015','AGIOI THEODOROI', 'GREECE','EMED'])

    def test_extract_dataframe(self):
        file_list = [
            'C:\\Dev\\Excel Files\\ICE_Settlement\\Attachments\\ICE 1630 SGT Brent Crude Futures curve on 14-Jan-20_20200114.csv'
        ]
        
        json = {
            "name":"ICE 1630 SGT Brent Crude Future",
            "columns":[
                {
                    "name":"",
                    "start":0,
                    "end":2,
                    "blocks":[
                        {
                            "name":"ICE_Settlement_Curve",
                            "y_start": 0,
                            "columns":[
                                {
                                    "name":"Curve"
                                },
                                {
                                    "name":"Asof", 
                                    "value":"file_date"                           
                                },
                                {
                                    "name":"ContractDate"
                                },
                                {
                                    "name":"Value"
                                }
                            ]
                        }
                    ]
                }
            ]
        }
        #TODO
        #self.assertIsNone(1)

class DataAccessCase(unittest.TestCase):
    def setUp(self):
        self.server = "Lon-PC53"
        self.database = "Analytics"
        
    # python -m unittest test_basic.DataAccessCase.test_loadToCSV
    def test_loadToCSV(self):
        dataAccess = DataAccess(self.server, self.database)
        self.assertIsNone(dataAccess.loadToCSV("Select * from Period"))

    # python -m unittest test_basic.DataAccessCase.test_bulkInsert
    def test_bulkInsert(self):
        dataAccess = DataAccess(self.server, 'IEAData')
        self.assertIsNone(dataAccess.bulkInsert('SUPPLY', "C:\Dev\Excel Files\Output\IEA\Supply.txt", truncate=True ))

    # python -m unittest test_basic.DataAccessCase.test_deleteById
    def test_deleteById(self):
        dataAccess = DataAccess(self.server, 'Analytics')
        self.assertIsNone(dataAccess.deleteById('RystadProduction', "Period", "'2019-02-01'" ))


    # python -m unittest test_basic.DataAccessCase.test_load
    def test_load(self):
        dataAccess = DataAccess(self.server, self.database)
        self.assertIsInstance(dataAccess.load('Commodity', Name = "Crude"), list)
        self.assertIsInstance(dataAccess.load('Commodity'), list)
        self.assertIsInstance(dataAccess.load('Commodity', Name = "Crude", Description=None), list) # TODO where is null

    # python -m unittest test_basic.DataAccessCase.test_load_with_date_filter
    def test_load_with_date_filter(self):
        dataAccess = DataAccess(self.server, self.database)

        dataAccess.cursor = MagicMock()
        dataAccess.cursor.execute.return_value = dataAccess.cursor

        quote = collections.namedtuple('quote', 'Instrument IdInstrument Asof ContractDate RelativePeriod Value')

        quote1 = quote('North Sea', 1961, '2020-02-01','2020-02-01', 1,100)
        
        dataAccess.cursor.fetchall.return_value = quote1
        filter = {'Instrument': 'North Sea', 'ContractCode': 'B' , '>=Asof': datetime.strftime(datetime(2019, 12, 6, 0, 0, 0), "%Y%M%d")}

        print(dataAccess.load('view_Quotes_Futures', **filter))

    def test_get_max_database_date(self):
        dataAccess = DataAccess(self.server, 'Price')
        d = {'Class':['VLCC']}
        self.assertEqual(type(dataAccess.get_max_database_date('McQuilling', 'DateStamp', 'import', **d)), datetime)


    def test_delete_two_parameters_one_value_each(self):
        dataAccess = DataAccess(self.server, 'Price')

        df = pd.DataFrame(
            {
                'asof': ['2020-01-14', '2020-01-14'],
                'curve': ['ICE 1630 LS Gas Oil Futures', 'ICE 1630 LS Gas Oil Futures']
            }
        )

        dict_keys = get_unique_values_for_dataframe_keys(df, ['asof', 'curve'])        
              
        self.assertIsNone(dataAccess.delete('import.ICE_Settlement_Curve', **dict_keys))

class EiaImporterCase(unittest.TestCase):
    def setUp(self):
        self.eiaImporter = eia.EiaImporter(
                server=os.environ['ADUB_DBServer'], 
                database='Analytics',
                url="http://api.eia.gov",
                api_key='a50a785e3c8ad1b5bdd26cf522d4d473',
                file_path="{}\\EIA".format(os.environ['ADUB_Import_Output_UNC']),
                bulkinsert_path="{}\\EIA".format(os.environ['ADUB_Import_Output'])
            )
        
        self.seriesString = 'PET.WCESTUS1.W;PET.WCESTP11.W;PET.WCESTP21.W;PET.WCESTP31.W;PET.WCESTP41.W;PET.WCESTP51.W;PET.WCRIMUS2.W;PET.WCEIMP12.W;PET.WCEIMP22.W;PET.WCEIMP32.W;PET.WCEIMP42.W;PET.WCEIMP52.W;PET.WCREXUS2.W;PET.WCREXUS42.W;PET.WCRRIUS2.W;PET.WCRRIP12.W;PET.WCRRIP22.W;PET.WCRRIP32.W;PET.WCRRIP42.W;PET.WCRRIP52.W;PET.WCRFPUS2.W;PET.WCSSTUS1.W'
        self.dataAccess = dtAccss.DataAccess(os.environ['ADUB_DBServer'], 'Analytics')

        self.eiaImporter.truncateAll()

    
    # python -m unittest test_EiaImporter.EiaImporterCase.test_runSeries
    def test_runSeries(self):              
        self.assertIsNone(self.eiaImporter.runSeries(self.seriesString))

    def test_runSeries_fromEiaSeriesShortname(self):              
        self.assertIsNone(self.eiaImporter.runSeries())

    def test_loadSeries(self):
        self.assertIsNone(self.eiaImporter.loadSeries(self.seriesString, None, self.dataAccess))


class EmailImporterCase(unittest.TestCase):
    def test_SGTBrentCrude(self):
        eiSGTBrentCrude.run()

    def test_1930LSGasOil(self):
        ei1930LSGasOil.run()

    def test_ei1630Oil(self):
        ei1630Oil.run()

    def test_ei1630BrentCurve(self):
        ei1630BrentCurve.run()

class ExcelExtractorCase(unittest.TestCase):
    def setUp(self):
        # template examples
        self.block_column1 = {
            "name":"column 1"
        }

        self.block_column2 = {
            "name":"column 2"
        }

        self.block_column3 = {
            "name":"column 3",
            "value":1
        }

        self.block_column4 = {
            "name":"Datestamp",
            "value":"date"
        }

        self.block1 = {
                "name":"block1", 
                "y_start":1,
                "columns":[self.block_column1, self.block_column2]
        }

        self.block2 = {
                "name":"block2", 
                "y_start":1,
                "columns":[self.block_column1, self.block_column2]
        }

        self.block3 = {
                "name":"block3", 
                "y_start":1,
                "columns":[
                    self.block_column1, self.block_column2, 
                    self.block_column3, self.block_column4
                ]
        }

        self.section1 = {
            "name":"section1",
            "start":0,
            "end":2,
            "blocks":[self.block1]
        }

        self.section2 = {
            "name":"section2",
            "start":3,
            "end":5,
            "blocks":[self.block2]
        }

        self.section3 = {
            "name":"section3",
            "start":0,
            "end":2,
            "blocks":[self.block1,self.block2]
        }


    def test_extract_base_case(self):
        self.assertEqual(extract_files(None, None), [])

    def test_extract_sections_one_section(self):
        json = {
            "name":"sheet",
            "columns":[self.section1]
        }

        check = [('block1', 1, 2),(None, 'Test','Two')]

        expect = [
            {
                'name':'block1',
                'rows':[
                    {'column 1': 1, 'column 2': 'Test'},
                    {'column 1': 2, 'column 2': 'Two'}
                ]
            }            
        ]
        self.assertEqual(_extract_sections(json["columns"], check), expect)

    def test_extract_sections_two_sections(self):
        json = {
            "name":"sheet",
            "columns":[
                self.section1,
                self.section2
            ]
        }

        check =  [
            (self.block1["name"], 1, 2),
            (None, 'section_1_a','section_1_b'),
            ('', '',''),
            (self.block2["name"], 3, 4),
            (None, 'section_2_a','section_2_b')
        ]
        
        expect = [            
            {
                'name':self.block1["name"],
                'rows':[
                    {'column 1': 1, 'column 2': 'section_1_a'},
                    {'column 1': 2, 'column 2': 'section_1_b'}
                ]
            },
            {
                'name':self.block2["name"],
                'rows':[
                    {'column 1': 3, 'column 2': 'section_2_a'},
                    {'column 1': 4, 'column 2': 'section_2_b'}
                ]
            }            
        ]

        self.assertEqual(_extract_sections(json["columns"], check), expect)

    def test_extract_files(self):
        file_list = [
            'C:\\Dev\\Projects\\Analytics\\Python37\\adub\\tests\\data\\test_excel_extractor.xlsx'
        ]
        
        json = {
            "name":"one column one block",
            "columns":[self.section1]
        }

        expect = [
            {
                'name':self.block1["name"],
                'rows':[
                    {'column 1': 1.0, 'column 2': 'a'}, 
                    {'column 1': 2.0, 'column 2': 'b'}, 
                    {'column 1': 3.0, 'column 2': 'c'}, 
                    {'column 1': 4.0, 'column 2': 'd'}, 
                    {'column 1': 5.0, 'column 2': 'e'},
                    {'column 1': 6.0, 'column 2': 'f'},
                    {'column 1': 7.0, 'column 2': 'g'},
                    {'column 1': 8.0, 'column 2': 'h'}
                ]
            }
        ]
        
        self.assertEqual(extract_files(file_list, json), expect)

    def test_get_block_start(self):
        data = list(
            zip(
                [
                    'VLCC', None , None],
                    [1, '' ,2],
                    ['a', '', 'b'],
                    ['test','','another']
                )
            )

        self.assertEqual(_get_block_start(data, 'VLCC', 0), 0)

    def test_get_block_end_one_block(self):
        data = list(zip([1],['a'],['test']))
        self.assertEqual(_get_block_end(data, 0), 1)

    def test_get_block_end_two_blocks(self):
        data = list(zip([1, None ,2],['a', '', 'b'],['test','','another']))
        self.assertEqual(_get_block_end(data, 0), 1)

    def test_get_value_datestamp(self):
        t = (1, '2', 'Test')
        c = self.block_column4
        
        i = 4

        self.assertEqual(
            _get_value(t, c, i), datetime.now().strftime("%d-%m-%Y")
        )

    def test_get_value_default(self):
        t = (1, '2', 'Test')
        c = self.block_column3
        
        i = 4

        self.assertEqual(_get_value(t, c, i), 1)

    def test_get_value_found(self):
        t = (1, '2', 'Test')
        c = self.block_column1
        
        i = 1

        self.assertEqual(_get_value(t, c, i), '2')
        

    def test_extract_columns(self):
        columns = [
            self.block_column1,
            self.block_column2,
            self.block_column3,
        ]
        values = list(zip([1],['a'],['test']))

        expect = {"column 1":1,"column 2":'a',"column 3":'test'}
        self.assertEqual(_extract_columns(columns, values[0]), expect)

    def test_extract_blocks_one_block(self):
        json = [
            self.block1
        ]

        check = [(self.block1["name"],'',''),(1,'a','test')]
        
        expect = [
            {
                'name':self.block1["name"], 
                'rows': [
                    {"column 1":1, "column 2":'a'}
                ]
            }
        ]

        self.assertEqual(_extract_blocks(json, check), expect)


    def test_extract_blocks_two_blocks(self):
        json = [
            self.block1,
            self.block2
        ]

        check  = [
            (self.block1["name"],'',''),
            (1,'a','test'),(2,'b','try'),
            ('','',''),(self.block2["name"],'',''),
            (12,'l','another')
        ]

        expect = [
            {
                'name':self.block1["name"], 
                'rows': [
                    {"column 1":1, "column 2":'a'},
                    {"column 1":2, "column 2":'b'}
                ]
            },
            {
                'name':self.block2["name"], 
                'rows': [
                    {"column 1":12, "column 2":'l'}
                ]
            }
        ]
        self.assertEqual(_extract_blocks(json, check), expect)

    def test_extract_rows(self):
        
        json = [ 
            self.block_column1,
            self.block_column2,
            self.block_column3,
            self.block_column4
        ]

        check = list(zip([1,2],['a','b']))

        expect = [
            {
                "column 1":1, 
                "column 2":'a', 
                "column 3":self.block_column3["value"], 
                'Datestamp':datetime.now().strftime("%d-%m-%Y")
            },
            {
                "column 1":2, 
                "column 2":'b', 
                "column 3":self.block_column3["value"], 
                'Datestamp':datetime.now().strftime("%d-%m-%Y")
            }
        ]

        self.assertEqual(_extract_rows(json, check), expect)


class ExchangeWrapperCase(unittest.TestCase):
    def setUp(self):
        self.exchangeWrapper = ExchangeWrapper()

        self.m = Message(
            account=self.exchangeWrapper.account,
            subject='Daily motivation',
            body='All bodies are beautiful',
            to_recipients=[
                Mailbox(email_address='anne@example.com'),
                Mailbox(email_address='bob@example.com'),
            ],
            datetime_received=datetime(2000, 1, 1, 12, 1, 30),
            cc_recipients=['carl@example.com', 'denice@example.com'],  # Simple strings work, too
            bcc_recipients=[
                Mailbox(email_address='erik@example.com'),
                'felicity@example.com',
            ],  # Or a mix of both
        )

        binary_file_content = 'Hello from unicode æøå'.encode('utf-8')  # Or read from file, BytesIO etc.

        a = FileAttachment(
            name='my_file.txt',
            content=binary_file_content
        )

        self.m.attach(a)
        self.file_path = 'C:\Dev\Excel Files\ExchangeWrapper'

        

    def test_get_start_date(self):
        
        date = datetime(2000, 1, 1, 12, 1, 30)
        self.assertEqual(date.year, self.exchangeWrapper.get_start_date(date).year)

    def test_get_emails(self):
        with mock.patch('services.exchangeWrapper.ExchangeWrapper.get_emails') as MockClass:
            MockClass.return_value = self.m.subject
            result = self.exchangeWrapper.get_emails('Daily motivation', datetime(2000, 1, 1, 12, 1, 30))
            self.assertEqual(result, self.m.subject)

    def test_save_email_attachments(self):
        self.exchangeWrapper.save_email_attachments((self.m,), self.file_path)
        time_delta = datetime.now() - datetime.utcfromtimestamp(os.path.getmtime(os.path.join(self.file_path, 'my_file_20000101.txt')))
        self.assertLess(time_delta.seconds, 100000)

    def test_save_attachment(self):
        self.exchangeWrapper.save_attachment(self.m.attachments[0], self.file_path, datetime(2000, 1, 1, 12, 1, 30))
        time_delta = datetime.now() - datetime.utcfromtimestamp(os.path.getmtime(os.path.join(self.file_path, 'my_file_20000101.txt')))
        self.assertLess(time_delta.seconds, 100000)

    

    def test_get_emails_live(self):
        
        for i in range (10):
            q = Queue()
            p = Process(target=get_email_bg, args=(q,i))
            p.start()
            time.sleep(1)

def get_email_bg(q, i):
    date = datetime(2020,2,5,12,0,0)
    exchangeWrapper = ExchangeWrapper()
    emails = exchangeWrapper.get_emails('ICE 1630 Oil Futures Curves', date)
    exchangeWrapper.save_email_attachments(list(emails), os.path.join('C:\Dev\Excel Files\ExchangeWrapper', 'ICE'))

class ICE_SettlementCleanerCase(unittest.TestCase):
    def setUp(self):
        self.d1 = pd.DataFrame({
            'ContractDate':['Mar20', 'Apr-20','May-20'],
            'Value':[1,2,3]
        })

        self.d2 = pd.DataFrame({
            'ContractDate':['Mar20', 'Apr-20','May-20'],
            'Value':[1,2,3],
            'Asof':['01-03-2020','01-03-2020','01-03-2020'],
            'Curve':['Curve 1', 'Curve 2', 'Curve 3']
        })

    def test_set_asof(self):
        self.assertEqual(_set_asof(self.d1, datetime(2020, 1, 21, 12, 0, 0))['Asof'].iloc[0], '20200121')

    def test_format_contract_column(self):
        self.assertEqual(_format_contract_column(self.d1)['ContractDate'].iloc[0], '20200301')

    def test_parse_contract_column(self):
        self.assertEqual(_parse_contract_column('Mar20'), '20200301')

    def test_set_curve_name_column(self):
        self.assertEqual(_set_curve_name_column(self.d1, 'Test')['Curve'].iloc[0], 'Test')

    def test_arrange_columns(self):
        self.assertEqual(list(_arrange_columns(self.d2).columns), ['Curve','Asof','ContractDate','Value'])

class IeaTxtCleanerCase(unittest.TestCase):
    # python -m unittest test_IeaTxtCleaner.IeaTxtCleanerCase.test_replaceInvalidQuantities
    def test_replaceInvalidQuantities(self):
        d = {'colA':[1,2,3], 'Quantity':['x', 200,300],'colC':['x','y','z']}
        df = pd.DataFrame(data=d)

        self.assertEqual(_replaceInvalidQuantities(df).iloc[0]['Quantity'], '0')

    

    # python -m unittest test_IeaTxtCleaner.IeaTxtCleanerCase.test__cleanDateColumns__
    def test__cleanDateColumns__(self):
        frequency_indicators = ['Month', 'Quarter','Year']

        dIn = {
            'Period':pd.Series(['JAN2019', '2Q2005','MAR-2018'],dtype='str'),
            'FREQUENCY':pd.Series(['Monthly','YEARLY','Quarterly'],dtype='str')
        }
        dfIn = pd.DataFrame(data=dIn)

        dOut = {
            'Period':pd.Series(['2019-01-01', '2005-04-01','2018-03-01'],dtype='datetime64[ns]'),
            'FREQUENCY':pd.Series(['Monthly','YEARLY','Quarterly'],dtype='str')
        }
        dfOut = pd.DataFrame(data=dOut)
        dfTest = _cleanDateColumns(dfIn)
        self.assertTrue(dfTest.equals(dfOut))   

    # python -m unittest test_IeaTxtCleaner.IeaTxtCleanerCase.test__setFrequency__
    def test__setFrequency__(self):
        dIn = {
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'Period':pd.Series(['1Q2015','3Q2014','2018','JAN2015','DEC2015'], dtype='str')
        }
        dfIn = pd.DataFrame(data=dIn)

        dOut = {
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'Period':pd.Series(['1Q2015','3Q2014','2018','JAN2015','DEC2015'], dtype='str'),
            'PeriodType':pd.Series(['Quarter','Quarter', 'Year', 'Month','Month'], dtype='str')
        }
        
        dfOut = pd.DataFrame(data=dOut)
        dfTest = _setFrequency(dfIn)
        
        self.assertTrue(dfTest.equals(dfOut))

    # python -m unittest test_IeaTxtCleaner.IeaTxtCleanerCase.test__setFrequency__for_new_csv_format
    def test__setFrequency__for_new_csv_format(self):
        dIn = {
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'FREQUENCY':pd.Series(['monthly','quarterly','monthly','yearly','quarterly'], dtype='str')
        }
        dfIn = pd.DataFrame(data=dIn)

        dOut = {
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'FREQUENCY':pd.Series(['monthly','quarterly','monthly','yearly','quarterly'], dtype='str'),
            'PeriodType':pd.Series(['Month','Quarter', 'Month', 'Year','Quarter'], dtype='str')
        }
        
        dfOut = pd.DataFrame(data=dOut)
        dfTest = _setFrequency(dfIn)
        
        self.assertTrue(dfTest.equals(dfOut))

    # python -m unittest test_IeaTxtCleaner.IeaTxtCleanerCase.test_clean
    def test_clean(self):
        dIn = {
            'Quantity':pd.Series([12,25,36,'x',55]),
            'Period':pd.Series(['1Q2015','3Q2014','2018','JAN2015','DEC2015'], dtype='str'),
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'Product':pd.Series(['CRUDE','NG','LNG','Stuff','Coal'], dtype='str')
        }
        dfIn = pd.DataFrame(data=dIn)

        dOut = {
            'Quantity':pd.Series([12,25,36,'0',55]),
            'Period':pd.Series(['2015-01-01','2014-07-01','2018-01-01','2015-01-01','2015-12-01'], dtype='datetime64[ns]'),
            'Country':pd.Series(['Aus','Ireland','UK','France','Congo'], dtype='str'),
            'Product':pd.Series(['CRUDE','NG','LNG','Stuff','Coal'], dtype='str'),
            'PeriodType':pd.Series(['Quarter','Quarter', 'Year', 'Month','Month'], dtype='str'),
            'Asof':pd.Series([date.today(),date.today(), date.today(), date.today(),date.today()])
        }
        dfOut = pd.DataFrame(data=dOut)
        dfTest = clean_iea(dfIn)
        self.assertTrue(dfTest.equals(dfOut))

        # now test for the new format from
        # IEA

        dIn_newFormat = {
            # 'FIELD':pd.Series(['field one','field two']),
            # 'COUNTRY':pd.Series(['country one','country two']),
            # 'PRODUCT':pd.Series(['product one','product two']),
            # 'ENVIRONMENT':pd.Series(['environment one','environment two']),
            'TIME':pd.Series(['01/09/1994','01/09/1994','01/09/1994'], dtype='str'),
            'FREQUENCY':pd.Series(['monthly','yearly','quarterly'], dtype='str'),
            'TIMESTAMP':pd.Series(['01/09/1994','01/09/1995','01/09/1996'], dtype='str'),
            'VALUE':pd.Series([1,2,3], dtype='str'),
        }
        dfIn_newFormat = pd.DataFrame(data=dIn_newFormat)

        dOut_newFormat = {
            'TIME':pd.Series(['01/09/1994','01/09/1994','01/09/1994'], dtype='str'),
            'FREQUENCY':pd.Series(['monthly','yearly','quarterly'], dtype='str'),
            'TIMESTAMP':pd.Series(['01/09/1994','01/09/1995','01/09/1996'], dtype='str'),
            'VALUE':pd.Series([1,2,3], dtype='str'),            
            'PeriodType':pd.Series(['Month','Year', 'Quarter'], dtype='str'),            
            'Asof':pd.Series([date.today(),date.today(), date.today()]),
            'Period':pd.Series(['1994-01-09','1995-01-09','1996-01-09'], dtype='datetime64[ns]')
        }
        dfOut_newFormat = pd.DataFrame(data=dOut_newFormat)
        dfTest_newFormat = clean_iea(dfIn_newFormat)

        self.assertTrue(dfTest_newFormat.equals(dfOut_newFormat))

    def test_remove_header_row(self):
        check = pd.DataFrame(
            {
                'header': ['header', 1, 2]
            }
        )
        self.assertEqual(_remove_header_row(check)['header'].iloc[0], 1)

class LogCase(unittest.TestCase):
        
    def test_log(self):
        self.assertIsInstance(old_log(__name__, 'test_log', "short function name"), str)
        self.assertIsInstance(old_log(__name__, 'really_long_function_name_to_test_alignment_log', "long function name"), str)
        self.assertIsInstance(old_log(__name__, 'really_long_function_name_to_test_alignment_log', "long function name", 'Error',True, "logCase"), str)

    def test_log_info(self):
        self.assertIsInstance(log.info(__name__, 'test_log', "short function name"), str)

    def test_log_warning(self):
        self.assertIsInstance(log.warning(__name__, 'test_log', "short function name"), str)

    def test_log_error(self):
        self.assertIsInstance(log.error(__name__, 'test_log', "short function name"), str)

class McQuillingCase(unittest.TestCase):
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

    # python -m unittest test_basic.McQuillingCase.test_get_attachments
    def test_get_attachments(self):
        config = Configuration( 
            service_endpoint=EXCHANGE_SERVER, credentials=Credentials(USERNAME, PASSWORD))
        account = Account(ANALYTICS_EMAIL_ADDRESS, config=config, access_type=DELEGATE)

        self.assertIsNone(
            self.mcQuilling.get_attachments(
                list(self.mcQuilling.get_emails(datetime.strptime(str(20191213), '%Y%m%d'), 'Daily Freight Rate Assessment', account)), self.mcQuilling.file_path))  

    # python -m unittest test_basic.McQuillingCase.test_run
    def test_run(self):
        self.assertIsNone(self.mcQuilling.run())

    # python -m unittest test_basic.McQuillingCase.test_run_with_bad_path
    def test_run_with_bad_path(self):
        temp_path = self.mcQuilling.file_path
        self.mcQuilling.file_path = "bad_path"
        self.assertIsNone(self.mcQuilling.run())
        self.mcQuilling.file_path = temp_path

    # python -m unittest test_basic.McQuillingCase.test_get_max_file_date
    def test_get_max_file_date(self):
        self.assertEqual(self.mcQuilling.get_max_file_date(self.file_list), 20191201)

    # python -m unittest test_basic.McQuillingCase.test_get_max_file_date_with_bad_file_name
    def test_get_max_file_date_with_bad_file_name(self):
        self.assertEqual(self.mcQuilling.get_max_file_date(self.bad_file_list), 20191201)

class RystadCleanerCase(unittest.TestCase):
    
    def setUp(self):
        self.testData = """
            2018|August|Not specified|United Kingdom|Crude Oil|Sweet|0.2|Production|10
            2018|August|Not specified|United Kingdom|Crude Oil|Slightly Sour|1.33|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.39|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.8|Production|10
        """
        self.df = pd.read_csv(StringIO(self.testData), header=None, sep='|',
                  names=['Year', 'Month', 'Crude Stream', 'Country', 'Oil and Gas Category', 'Sulphur Group', 'Sulphur Detail','[Data Values]', 'Sum'])
    
    # python -m unittest test_rystadCleaner.RystadCleanerCase.test__getWeightedSulphur__
    def test__getWeightedSulphur__(self):
        testData = """
            2018|August|Not specified|United Kingdom|Crude Oil|Sweet|0.2|Production|10
            2018|August|Not specified|United Kingdom|Crude Oil|Slightly Sour|1.33|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.39|Production|10
            2018|August|Anasuria|United Kingdom|Crude Oil|Sweet|0.8|Production|10
        """
        df = pd.read_csv(StringIO(self.testData), header=None, sep='|',
                  names=['Year', 'Month', 'Crude Stream', 'Country', 'Oil and Gas Category', 'Sulphur Group', 'Sulphur Detail','[Data Values]', 'Sum'])

        newCols = ['Year','Month','Crude Stream','Country','Oil and Gas Category','Sulphur Group','Sulphur Detail','[Data Values]','Sum_x','ProductionSulphur_x','ProductionSulphur_y','Sum_y','WeightedSulphur']
        self.assertEqual(__getWeightedSulphur__(df).columns.tolist(), newCols)
    
    # python -m unittest test_rystadCleaner.RystadCleanerCase.test__setWeightedSulphur__
    def test__setWeightedSulphur__(self):
        self.assertEqual(__setWeightedSulphur__(self.df.iloc[0]), 0.2)

     # python -m unittest test_rystadCleaner.RystadCleanerCase.test_clean
    def test_clean(self):   
        dataIn = """
            2019|January|Alabama Sweet|United States|Crude Oil|Sweet|0.4|Production|8.956806
            2019|January|Alabama Sweet|United States|Condensate|Sweet|0.4|Production|5.154038
        """
        dfIn = pd.read_csv(StringIO(dataIn), header=None, sep='|',
                  names=['Year', 'Month', 'Crude Stream', 'Country', 'Oil and Gas Category', 'Sulphur Group', 'Sulphur Detail','[Data Values]', 'Sum'])

        dataOut = """United States|Crude Oil|Alabama Sweet|2019-01-01|8.956806|Sweet|0.4|0.4\r\nUnited States|Condensate|Alabama Sweet|2019-01-01|5.154038|Sweet|0.4|0.4
        """
        dfOut = pd.read_csv(StringIO(dataOut), header=None, sep='|',
                  names=[ 'Country', 'Oil and Gas Category', 'Crude Stream', 'Period','Sum_x', 'Sulphur Group', 'Sulphur Detail','WeightedSulphur'])

        dfResult = clean(dfIn)

        #print (f"Expected\n {dfOut}")
        #print (f"Actual\n {dfResult}")
        self.assertEqual(dfResult.columns.tolist(),dfOut.columns.tolist())
        self.assertEqual(dfResult['Sum_x'].iloc[0], 8.956806)

class UtilsCase(unittest.TestCase):
    def setUp(self):
        self.file_path = 'C:\\Dev\\Projects\\Analytics\\Python37\\adub\\tests\\data'

    def test_get_date_from_string_no_date(self):        
        date = get_date_from_string("no_date")
        self.assertEqual(date, 20000101)

    def test_get_date_from_string_long_string(self):        
        date = get_date_from_string("ICE 1630 SGT Brent Crude Futures curve on 14-Jan-20_20200101")
        self.assertGreater(date, 20000101)

    def test_get_date_from_string_YYYYMMDD(self):        
        date = get_date_from_string("date_20010312")
        self.assertGreater(date, 20000101)

    def test_get_date_from_string_dd_MMM_YY(self):        
        date = get_date_from_string("date_14-Jan-20")
        self.assertGreater(date, 20000101)

    def test_get_date_from_string_dd_MMM_YYYY(self):        
        date = get_date_from_string("date_14-Jan-2020")
        self.assertGreater(date, 20000101)

    def test_get_date_from_string_ddMMYY_with_connected_string(self):        
        date = get_date_from_string("140120M2M")
        self.assertGreater(date, 20000101)

    def test_insert_datestamp_in_filename(self):
        string = insert_datestamp_in_filename('C:\Dev', 'Test.xlsx', datetime(2000, 1, 1, 12, 1, 1))
        self.assertEqual(string, 'C:\Dev\Test_20000101.xlsx')

    def test_load_json(self):
        test_data = {'name': 'ASSESSMENTS'}
        
        self.assertEqual(
            test_data, 
            load_json('C:\\Dev\\Projects\\Analytics\\Python37\\adub\\tests\\data\\test.json'))

    def test_load_json_invalid_file(self):
        try:
            load_json('C:\\Dev\\Projects\\Analytics\\Python37\\adub\\tests\\data\\test')
        except ValueError as e:
            self.assertEqual(
                ValueError, 
                type(e))

    def test_get_max_file_date(self):
        list_string = ["no_date","date_14-Jan-17","date_16-Jan-2018","140119M2M"]
        self.assertEqual(get_max_file_date(list_string), 20190114)

    def test_write_to_csv(self):
        write_to_csv([{},], os.path.join(self.file_path, 'test.csv'))
        time_delta = datetime.now() - datetime.utcfromtimestamp(os.path.getmtime(os.path.join(self.file_path, 'test.csv')))
        self.assertLess(time_delta.seconds, 100000)

    def test_files_later_than(self):
        list_string = ["no_date","date_14-Jan-17","date_16-Jan-2018","140119M2M"]
        list_string_result = ["date_16-Jan-2018","140119M2M"]
        self.assertEqual(files_later_than(list_string, datetime(2017, 12, 1, 12, 1, 1)), list_string_result)

    def test_get_unique_values_for_dataframe_keys(self):
        df = pd.DataFrame(
            {
                'id': [1,2,3],
                'date': ['2010-01-01','2010-01-01','2010-01-01'],
                'value': ['a','b','c']
            }
        )

        keys = ['id', 'date']

        expect = {
            'id': ['1','2','3'],
            'date': ['20100101'],
        }
        print(get_unique_values_for_dataframe_keys(df, keys))
        self.assertEqual(get_unique_values_for_dataframe_keys(df, keys), expect)

    def test_get_unique_values_for_dataframe_keys_YYYYMMDD_wrong_order_keys(self):
        df = pd.DataFrame(
            {
                'id': [1,2,3],
                'date': ['20100122','20100122','20100102'],
                'value': ['a','b','c']
            }
        )

        keys = ['value', 'date']

        expect = {
            'value':['a','b','c'],
            'date': ['20100122','20100102'],
        }
        print(get_unique_values_for_dataframe_keys(df, keys))
        self.assertEqual(get_unique_values_for_dataframe_keys(df, keys), expect)

    def test_get_unique_values_for_dataframe_keys_YYYYMMDD(self):
        df = pd.DataFrame(
            {
                'id': [1,2,3],
                'date': ['20100122','20100122','20100102'],
                'value': ['a','b','c']
            }
        )

        keys = ['date','value']

        expect = {
            
            'date': ['20100122','20100102'],
            'value':['a','b','c']
        }
        print(get_unique_values_for_dataframe_keys(df, keys))
        self.assertEqual(get_unique_values_for_dataframe_keys(df, keys), expect)


class priceReturnsCase(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None
        # Data definitions
        quote = collections.namedtuple('quote', 'IdInstrument Asof ContractDate RelativePeriod Value')

        ## Quote is struct(int date date int float)
        ## interp. a price value for an instrument on a given asof date

        self.quote1 = quote(1961,'2020-02-01','2020-02-01', 1,100)
        self.quote2 = quote(1961,'2020-02-01','2020-03-01', 2,110)
        self.quote3 = quote(1961,'2020-01-31','2020-01-01', 1,90)
        self.quote4 = quote(1961,'2020-01-31','2020-02-01', 2,80)
        self.quote5 = quote(1962,'2020-01-31','2020-02-01', 2,80)
        
        self.quote6 = quote(1961,'2020-01-30','2020-02-01', 2,70)
        self.quote7 = quote(1961,'2020-01-30','2020-02-01', 1,60)
        self.quote8 = quote(1961,'2020-01-31','2020-03-01', 3,50)

        self.quote9 = quote(1961,'2020-02-27','2020-02-01', 1,40)
        self.quote9a = quote(1961,'2020-02-27','2020-03-01', 2,45)
        self.quote10 = quote(1961,'2020-02-28','2020-02-01', 1,50)
        self.quote10a = quote(1961,'2020-02-28','2020-03-01', 2,55)
        self.quote10b = quote(1961,'2020-02-28','2020-04-01', 3,58)
        
        self.quote11 = quote(1961,'2020-03-02','2020-03-01', 1,60)
        self.quote12 = quote(1961,'2020-03-02','2020-04-01', 2,70)
        self.quote13 = quote(1961,'2020-04-02','2020-04-01', 1,80)
        self.quote14 = quote(1961,'2020-03-31','2020-04-01', 2,85)
        self.quote14a = quote(1961,'2020-03-31','2020-05-01', 3,85)
        self.quote15 = quote(1961,'2020-04-01','2020-05-01', 2,90)

        def fn_for_quote(q):
            q.Instrument        # str
            q.IdInstrument      # int
            q.Asof              # date
            q.ContractDate      # date
            q.RelativePeriod    # int
            q.Value             # float

        
        ## ListOfQuote is one of:
        ## - empty
        ## - list(Quote)
        ## interp. a list of Quotes

        self.loq0 = []
        self.loq1 = [self.quote1]
        self.loq2 = [self.quote3,self.quote4,self.quote1,self.quote2]

        def fn_for_loq(loq):
            if(loq == []):
                return []
            else:
                first = loq[0]
                rest = loq[1:]
                fn_for_quote(first)
                fn_for_loq(rest) 


        expiry = collections.namedtuple('expiry', 'Instrument Contract ExpiryDate')

        ## expiry is struct(str date date) | False
        ## interp. the expiry date for a given instrument and contract

        self.expiry0 = False
        self.expiry1 = expiry('North Sea','2020-01-01','2020-01-31')
        self.expiry2 = expiry('North Sea','2020-02-01','2020-02-28')
        self.expiry3 = expiry('North Sea','2020-03-01','2020-03-31')

        def fn_for_expiry(e):
            if(e is False):
                return False
            else:            
                e.Instrument    # str
                e.Contract      # date
                e.ExpiryDate       # date

        ## ListOfExpiry is one of:
        ## - None
        ## - list(Expiry)
        ## interp. a list of Expiries

        self.loe0 = []
        self.loe1 = [self.expiry1,self.expiry2,self.expiry3]
        self.loe2 = [self.expiry3,self.expiry2,self.expiry1]
        self.loe3 = [self.expiry2,self.expiry3,self.expiry1]

        def fn_for_loe(loe):
            if(loe == []):
                return False
            else:
                # do something with...
                first = loe[0]
                rest = loe[1:]
                fn_for_expiry(first)
                fn_for_loe(rest) 

        ## Value is {str date int float}
        ## interp: a value on a curve

        self.value1 = {
            'Instrument': self.quote1.IdInstrument,
            'Asof': self.quote1.Asof,                    
            'M': 1,
            'Value': self.quote1.Value
        }

        self.value2 = {
            'Instrument': self.quote2.IdInstrument,
            'Asof': self.quote2.Asof,                    
            'M': 2,
            'Value': self.quote2.Value
        }

        self.value3 = {
            'Instrument': self.quote3.IdInstrument,
            'Asof': self.quote3.Asof,                    
            'M': 1,
            'Value': self.quote3.Value
        }


        self.value4 = {
            'Instrument': self.quote4.IdInstrument,
            'Asof': self.quote4.Asof,                    
            'M': 2,
            'Value': self.quote4.Value
        }

        

        self.value7 = {
            'Instrument': self.quote7.IdInstrument,
            'Asof': self.quote7.Asof,                    
            'M': 1,
            'Value': self.quote7.Value
        }

        self.value8 = {
            'Instrument': self.quote8.IdInstrument,
            'Asof': self.quote8.Asof,                    
            'M': 3,
            'Value': self.quote8.Value
        }

        self.value9a = {
            'Instrument': self.quote9a.IdInstrument,
            'Asof': self.quote9a.Asof,                    
            'M': 2,
            'Value': self.quote9a.Value
        }

        self.value9 = {
            'Instrument': self.quote9.IdInstrument,
            'Asof': self.quote9.Asof,                    
            'M': 1,
            'Value': self.quote9.Value
        }

        self.value10a = {
            'Instrument': self.quote10a.IdInstrument,
            'Asof': self.quote10a.Asof,                    
            'M': 2,
            'Value': self.quote10a.Value
        }

        
        self.value10b = {
            'Instrument': self.quote10b.IdInstrument,
            'Asof': self.quote10b.Asof,                    
            'M': 3,
            'Value': self.quote10b.Value
        }

        self.value10 = {
            'Instrument': self.quote10.IdInstrument,
            'Asof': self.quote10.Asof,                    
            'M': 1,
            'Value': self.quote10.Value
        }

        self.value11 = {
            'Instrument': self.quote11.IdInstrument,
            'Asof': self.quote11.Asof,                    
            'M': 1,
            'Value': self.quote11.Value
        }

        self.value12 = {
            'Instrument': self.quote12.IdInstrument,
            'Asof': self.quote12.Asof,                    
            'M': 2,
            'Value': self.quote12.Value
        }

        self.value13 = {
            'Instrument': self.quote13.IdInstrument,
            'Asof': self.quote13.Asof,                    
            'M': 1,
            'Value': self.quote13.Value
        }

        self.value14 = {
            'Instrument': self.quote14.IdInstrument,
            'Asof': self.quote14.Asof,                    
            'M': 2,
            'Value': self.quote14.Value
        }

        self.value14a = {
            'Instrument': self.quote14a.IdInstrument,
            'Asof': self.quote14a.Asof,                    
            'M': 3,
            'Value': self.quote14a.Value
        }

        self.value15 = {
            'Instrument': self.quote15.IdInstrument,
            'Asof': self.quote15.Asof,                    
            'M': 2,
            'Value': self.quote15.Value
        }

        ## ListValues is one of    
        ## - None
        ## - list(Value)
        ## interp. a list of Values

        self.lov0 = []
        self.lov1 = [self.value4, self.value1]
        self.lov2 = [self.value4, self.value1]

        def fn_for_lov(lov):
            if(lov == []):
                return []
            else:
                # do something with...
                first = lov[0]
                rest = lov[1:]
                fn_for_expiry(first)
                fn_for_lov(rest) 

    # TESTS     
    def test_log_returns_1(self):
        self.assertEqual(log_returns(1, 1), 0)

    def test_log_returns_2(self):
        v1 = 1
        v2 = 1.1
        self.assertEqual(log_returns(v1, v2), self.calc_returns(v1, v2))

    def test_get_continuation_curves_base_case(self):
        self.assertEqual(get_continuation_curves(self.loq0), [])
    
    def test_get_continuation_curves_simple_case(self): 
        self.assertEqual(get_continuation_curves(self.loq2), 
            [
                [              
                    {
                        'Instrument': self.quote3.IdInstrument,
                        'Asof': self.quote3.Asof,                    
                        'M': 1,
                        'Value': self.quote3.Value
                    },{
                        'Instrument': self.quote1.IdInstrument,
                        'Asof': self.quote1.Asof,                    
                        'M': 1,
                        'Value': self.quote1.Value
                    }
                    
                ],
                [  
                    {
                        'Instrument': self.quote4.IdInstrument,
                        'Asof': self.quote4.Asof,                    
                        'M': 2,
                        'Value': self.quote4.Value
                    },
                    {
                        'Instrument': self.quote2.IdInstrument,
                        'Asof': self.quote2.Asof,                    
                        'M': 2,
                        'Value': self.quote2.Value
                    }                    
                ]
            ]
        )

    def test_get_continuation_curve_empty(self):
        self.assertEqual(get_continuation_curve(1, self.loq0), [])

    def test_get_continuation_curve_first(self):
        
        self.assertEqual(get_continuation_curve(1, self.loq2), 
            [
                 {
                    'Instrument': self.quote3.IdInstrument,
                    'Asof': self.quote3.Asof,                    
                    'M': 1,
                    'Value': self.quote3.Value

                },
                {
                    'Instrument': self.quote1.IdInstrument,
                    'Asof': self.quote1.Asof,                    
                    'M': 1,
                    'Value': self.quote1.Value
                }
            ]
        )

    def test_get_continuation_curve_second(self):
        self.assertEqual(get_continuation_curve(2, self.loq2), 
            [
                {
                    'Instrument': self.quote4.IdInstrument,
                    'Asof': self.quote4.Asof,                    
                    'M': 2,
                    'Value': self.quote4.Value
                },
                {
                    'Instrument': self.quote2.IdInstrument,
                    'Asof': self.quote2.Asof,                    
                    'M': 2,
                    'Value': self.quote2.Value
                }
            ]
        )

    def test_is_series_quote_1(self):
        self.assertEqual(is_series_quote(1, self.quote1), True)

    def test_is_series_quote_2(self):
        self.assertEqual(is_series_quote(2, self.quote2), True)

    def test_is_series_quote_3(self):
        self.assertEqual(is_series_quote(1, self.quote3), True)

    def test_is_series_quote_4(self):
        self.assertEqual(is_series_quote(2, self.quote4), True)

    def test_get_as_series_item(self):
        self.assertEqual(get_as_series_item(1, self.quote1), [
                {
                    'Instrument': self.quote1.IdInstrument,
                    'Asof': self.quote1.Asof,                    
                    'M': 1,
                    'Value': self.quote1.Value
                }
            ]
        )

    def test_get_expiry_quotes_base_case(self):
        self.assertEqual(get_expiry_quotes(self.lov0, self.loe0), [])

    # self.quote9 = quote(1961,'2020-02-27','2020-02-01', 1,40)
    # self.quote9a = quote(1961,'2020-02-27','2020-03-01', 2,45)
    # self.quote10 = quote(1961,'2020-02-28','2020-02-01', 1,50)
    # self.quote10a = quote(1961,'2020-02-28','2020-03-01', 2,55)
    # self.quote11 = quote(1961,'2020-03-02','2020-03-01', 1,60)
    # self.quote12 = quote(1961,'2020-03-02','2020-04-01', 2,70)
    # self.quote13 = quote(1961,'2020-04-02','2020-04-01', 1,80)

    def calc_returns(self, v1, v2):
        #return numpy.log(v1/v2)
        return v1 - v2

    def test_get_expiry_quotes_julians_case(self):
        self.assertEqual(get_expiry_quotes([self.value11,self.value10a], self.loe1), [self.value10a])   

    # self.quote9 = quote(1961,'2020-02-27','2020-02-01', 1,40)
    # self.quote9a = quote(1961,'2020-02-27','2020-03-01', 2,45)
    # self.quote10 = quote(1961,'2020-02-28','2020-02-01', 1,50)
    # self.quote10a = quote(1961,'2020-02-28','2020-03-01', 2,55)
    # self.quote11 = quote(1961,'2020-03-02','2020-03-01', 1,60)
    # self.quote12 = quote(1961,'2020-03-02','2020-04-01', 2,70)
    # self.quote13 = quote(1961,'2020-04-02','2020-04-01', 1,80)
    # self.quote14 = quote(1961,'2020-03-31','2020-04-01', 2,85)
    # self.quote14a = quote(1961,'2020-03-31','2020-05-01', 3,85)
    # self.quote15 = quote(1961,'2020-04-01','2020-05-01', 2,90)

    def test_get_returns_for_curve_day_of_expiry_no_shift(self):
        self.assertEqual(get_returns_for_curve([self.value10,self.value9], [self.value10a]),
            [
                {
                    'Instrument': self.quote10.IdInstrument,
                    'Asof': self.quote10.Asof,                    
                    'M': 1,
                    'Value': self.calc_returns(self.quote10.Value, self.quote9.Value)
                } ,
                {
                    'Instrument': self.quote9.IdInstrument,
                    'Asof': self.quote9.Asof,                    
                    'M': 1,
                    'Value': self.calc_returns(self.quote9.Value, 1)
                }                
            ]
        )

    # self.quote9 = quote(1961,'2020-02-27','2020-02-01', 1,40)
    # self.quote9a = quote(1961,'2020-02-27','2020-03-01', 2,45)
    # self.quote10 = quote(1961,'2020-02-28','2020-02-01', 1,50)
    # self.quote10a = quote(1961,'2020-02-28','2020-03-01', 2,55)
    # self.quote11 = quote(1961,'2020-03-02','2020-03-01', 1,60)
    # self.quote12 = quote(1961,'2020-03-02','2020-04-01', 2,70)
    # self.quote13 = quote(1961,'2020-04-02','2020-04-01', 1,80)
    # self.quote14 = quote(1961,'2020-03-31','2020-04-01', 2,85)
    # self.quote14a = quote(1961,'2020-03-31','2020-05-01', 3,85)
    # self.quote15 = quote(1961,'2020-04-01','2020-05-01', 2,90)

    def test_get_returns_for_curve_day_after_expiry_shift(self):
        self.assertEqual(get_returns_for_curve([self.value11,self.value10], [self.value10a]),
            [
                {
                    'Instrument': self.quote11.IdInstrument,
                    'Asof': self.quote11.Asof,                    
                    'M': 1,
                    'Value': self.calc_returns(self.quote11.Value, self.quote10a.Value)
                } ,
                {
                    'Instrument': self.quote10.IdInstrument,
                    'Asof': self.quote10.Asof,                    
                    'M': 1,
                    'Value': self.calc_returns(self.quote10.Value, 1)
                }                
            ]
        )

    # self.quote9 = quote(1961,'2020-02-27','2020-02-01', 1,40)
    # self.quote9a = quote(1961,'2020-02-27','2020-03-01', 2,45)
    # self.quote10 = quote(1961,'2020-02-28','2020-02-01', 1,50)
    # self.quote10a = quote(1961,'2020-02-28','2020-03-01', 2,55)
    # self.quote11 = quote(1961,'2020-03-02','2020-03-01', 1,60)
    # self.quote12 = quote(1961,'2020-03-02','2020-04-01', 2,70)
    # self.quote13 = quote(1961,'2020-04-02','2020-04-01', 1,80)
    # self.quote14 = quote(1961,'2020-03-31','2020-04-01', 2,85)
    # self.quote14a = quote(1961,'2020-03-31','2020-05-01', 3,85)
    # self.quote15 = quote(1961,'2020-04-01','2020-05-01', 2,90)

    def test_get_returns_for_curve_day_after_expiry_double_proxy_shift(self):
        self.assertEqual(get_returns_for_curve([self.value11,self.value10], [self.value10a], \
            [self.value12,self.value10a], is_double_proxy=True),
            [
                {
                    'Instrument': self.quote11.IdInstrument,
                    'Asof': self.quote11.Asof,                    
                    'M': 1,
                    'Value': self.calc_returns(self.quote12.Value, self.quote10a.Value)
                } ,
                {
                    'Instrument': self.quote10.IdInstrument,
                    'Asof': self.quote10.Asof,                    
                    'M': 1,
                    'Value': self.calc_returns(self.quote10.Value, 1)
                }                
            ]
        )

    # self.quote9 = quote(1961,'2020-02-27','2020-02-01', 1,40)
    # self.quote9a = quote(1961,'2020-02-27','2020-03-01', 2,45)
    # self.quote10 = quote(1961,'2020-02-28','2020-02-01', 1,50)
    # self.quote10a = quote(1961,'2020-02-28','2020-03-01', 2,55)
    # self.quote11 = quote(1961,'2020-03-02','2020-03-01', 1,60)
    # self.quote12 = quote(1961,'2020-03-02','2020-04-01', 2,70)
    # self.quote13 = quote(1961,'2020-04-02','2020-04-01', 1,80)
    # self.quote14 = quote(1961,'2020-03-31','2020-04-01', 2,85)
    # self.quote14a = quote(1961,'2020-03-31','2020-05-01', 3,85)
    # self.quote15 = quote(1961,'2020-04-01','2020-05-01', 2,90)

    def test_get_returns_for_curve_two_days_after_expiry_no_shift(self):
        self.assertEqual(get_returns_for_curve([self.value13,self.value11], [self.value10a]),
            [
                {
                    'Instrument': self.quote13.IdInstrument,
                    'Asof': self.quote13.Asof,                    
                    'M': 1,
                    'Value': self.calc_returns(self.quote13.Value, self.quote11.Value)
                } ,
                {
                    'Instrument': self.quote11.IdInstrument,
                    'Asof': self.quote11.Asof,                    
                    'M': 1,
                    'Value': self.calc_returns(self.quote11.Value, 1)
                }                
            ]
        )

    # self.quote9 = quote(1961,'2020-02-27','2020-02-01', 1,40)
    # self.quote9a = quote(1961,'2020-02-27','2020-03-01', 2,45)
    # self.quote10 = quote(1961,'2020-02-28','2020-02-01', 1,50)
    # self.quote10a = quote(1961,'2020-02-28','2020-03-01', 2,55)
    # self.quote11 = quote(1961,'2020-03-02','2020-03-01', 1,60)
    # self.quote12 = quote(1961,'2020-03-02','2020-04-01', 2,70)
    # self.quote13 = quote(1961,'2020-04-02','2020-04-01', 1,80)
    # self.quote14 = quote(1961,'2020-03-31','2020-04-01', 2,85)
    # self.quote14a = quote(1961,'2020-03-31','2020-05-01', 3,85)
    # self.quote15 = quote(1961,'2020-04-01','2020-05-01', 2,90)

    def test_get_returns_for_curve_day_of_expiry_m2_shift(self):
        self.assertEqual(get_returns_for_curve([self.value15,self.value14], [self.value14a]),
            [
                {
                    'Instrument': self.quote15.IdInstrument,
                    'Asof': self.quote15.Asof,                    
                    'M': 2,
                    'Value': self.calc_returns(self.quote15.Value, self.quote14a.Value)
                } ,
                {
                    'Instrument': self.quote14.IdInstrument,
                    'Asof': self.quote14.Asof,                    
                    'M': 2,
                    'Value': self.calc_returns(self.quote14.Value, 1)
                }                
            ]
        )

    # self.quote9 = quote(1961,'2020-02-27','2020-02-01', 1,40)
    # self.quote9a = quote(1961,'2020-02-27','2020-03-01', 2,45)
    # self.quote10 = quote(1961,'2020-02-28','2020-02-01', 1,50)
    # self.quote10a = quote(1961,'2020-02-28','2020-03-01', 2,55)
    # self.quote10b = quote(1961,'2020-02-28','2020-04-01', 3,58)
    # self.quote11 = quote(1961,'2020-03-02','2020-03-01', 1,60)
    # self.quote12 = quote(1961,'2020-03-02','2020-04-01', 2,70)
    # self.quote13 = quote(1961,'2020-04-02','2020-04-01', 1,80)
    # self.quote14 = quote(1961,'2020-03-31','2020-04-01', 2,85)
    # self.quote14a = quote(1961,'2020-03-31','2020-05-01', 3,85)
    # self.quote15 = quote(1961,'2020-04-01','2020-05-01', 2,90)

    def test_get_returns_for_curve_day_of_expiry_m2_day_of_expiry_no_shift(self):
        self.assertEqual(get_returns_for_curve([self.value10a,self.value9a], [self.value10b]),
            [
                {
                    'Instrument': self.quote10a.IdInstrument,
                    'Asof': self.quote10a.Asof,                    
                    'M': 2,
                    'Value': self.calc_returns(self.quote10a.Value, self.quote9a.Value)
                } ,
                {
                    'Instrument': self.quote9a.IdInstrument,
                    'Asof': self.quote9a.Asof,                    
                    'M': 2,
                    'Value': self.calc_returns(self.quote9a.Value, 1)
                }                
            ]
        )

    def test_get_returns_for_curves_julians_case(self):
        # self.quote9 = quote(1961,'2020-02-27','2020-02-01', 1,40)
        # self.quote9a = quote(1961,'2020-02-27','2020-03-01', 2,45)
        # self.quote10 = quote(1961,'2020-02-28','2020-02-01', 1,50)
        # self.quote10a = quote(1961,'2020-02-28','2020-03-01', 2,55)
        # self.quote10b = quote(1961,'2020-02-28','2020-04-01', 3,58)
        # self.quote11 = quote(1961,'2020-03-02','2020-03-01', 1,60)
        # self.quote12 = quote(1961,'2020-03-02','2020-04-01', 2,70)
        # self.quote13 = quote(1961,'2020-04-02','2020-04-01', 1,80)
        # self.quote14 = quote(1961,'2020-03-31','2020-04-01', 2,85)
        # self.quote14a = quote(1961,'2020-03-31','2020-05-01', 3,85)
        # self.quote15 = quote(1961,'2020-04-01','2020-05-01', 2,90)
        self.assertEqual(get_returns_for_curves([self.quote13, self.quote12, self.quote11, self.quote10b, self.quote10, self.quote10a, self.quote9a], [self.expiry1, self.expiry2, self.expiry3]),
            [
                [
                    {
                        'Instrument': self.quote13.IdInstrument,
                        'Asof': self.quote13.Asof,                    
                        'M': 1,
                        'Value': self.calc_returns(self.quote13.Value, self.quote11.Value),
                    },{
                        'Instrument': self.quote11.IdInstrument,
                        'Asof': self.quote11.Asof,                    
                        'M': 1,
                        'Value':self.calc_returns(self.quote11.Value, self.quote10a.Value) 
                    },
                    {
                        'Instrument': self.quote10.IdInstrument,
                        'Asof': self.quote10.Asof,                    
                        'M': 1,
                        'Value': self.calc_returns(self.quote10.Value, 1) 
                    }
                ],
                [
                     {
                        'Instrument': self.quote12.IdInstrument,
                        'Asof': self.quote12.Asof,                    
                        'M': 2,
                        'Value': self.calc_returns(self.quote12.Value, self.quote10b.Value) 
                    },
                    {
                        'Instrument': self.quote10a.IdInstrument,
                        'Asof': self.quote10a.Asof,                    
                        'M': 2,
                        'Value': self.calc_returns(self.quote10a.Value, self.quote9a.Value) 
                    },
                    {
                        'Instrument': self.quote9a.IdInstrument,
                        'Asof': self.quote9a.Asof,                    
                        'M': 2,
                        'Value': self.calc_returns(self.quote9a.Value, 1) 
                    }
                ],
                [
                     {
                        'Instrument': self.quote10b.IdInstrument,
                        'Asof': self.quote10b.Asof,                    
                        'M': 3,
                        'Value': self.calc_returns(self.quote10b.Value, 1) 
                    }
                ]
            ]
        )


    
    
   




    



       


