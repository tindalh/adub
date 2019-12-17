from io import StringIO
import unittest
import json
import pandas as pd
import sys
import os
sys.path.append('..')
import helpers.dataAccess as dtAccss
import helpers.csvHelper as csvHlpr
import importers.clipperDataImporter as clpprDtaImprtr


def get_json_result(url):
        return json.dumps("")

class TestClipperDataImporter(unittest.TestCase):
    # str -> json
    # Consumes a url and produces the resulting json

    # python -m unittest test_clipperDataImporter.TestClipperDataImporter.test_get_json_result
    def test_get_json_result(self):
        self.assertEqual(get_json_result(""), json.dumps(""))
    
    # python -m unittest test_clipperDataImporter.TestClipperDataImporter.test_import

    

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