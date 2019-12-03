import requests
from requests.auth import HTTPBasicAuth
import csv
import json
import sys
from helpers.analyticsEmail import sendEmail
import helpers.dataAccess as dtAccss


class ClipperDataImporter(object):
    def __init__(self, url, user, password, output_file_path, table_name, server, database):
        self.url = "http://appserver.clipperdata.com:8080/ClipperDataAPI-2/rest/clipperapi/data/"
        self.user = "targo.support@arcpet.co.uk"
        self.password = "arcsupport212"
        self.server = server,
        self.database = database
        self.table_name = table_name
        self.maxId = 0
        self.output_file_path = output_file_path

    def truncateCSV(self, file_path):
        f = open(file_path, "w+")
        f.close()

    def writeToCSV(self, rows, file_path):
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        
            for row in rows:
                writer.writerow(row)
    
    def writeDictToCSV(self, dict, file_path):
        with open(file_path, 'a', newline='') as csvfile:            
            writer = csv.DictWriter(csvfile, fieldnames=dict.keys(), delimiter='|', quoting=csv.QUOTE_MINIMAL)        
            writer.writerow(dict)

    def addToCSV(self, file_path, listRows):
        rowCount = 0
        for dictRow in listRows:
            dictRow['dateNum'] = dictRow['measuresGlobalCrudeEntityPK']['dateNum']
            dictRow['rowNum'] = dictRow['measuresGlobalCrudeEntityPK']['rowNum']
            dictRow['statNum'] = dictRow['measuresGlobalCrudeEntityPK']['statNum']
            dictRow.pop('measuresGlobalCrudeEntityPK', None)

            if(dictRow['rowNum'] > self.maxId):
                self.maxId = dictRow['rowNum']

            if(dictRow['statNum'] == 0):
                file_name = f"{self.csv_file_path}\\ClipperData.csv"
                if(rowCount == 0):
                    self.writeToCSV((dictRow.keys(),), file_name)

                self.writeDictToCSV(dictRow, file_name)
                
            else:
                file_name = f"{self.csv_file_path}\\ClipperData_Delete.csv"

                self.writeDictToCSV({"rowNum": dictRow['rowNum']}, file_name)

            rowCount += 1
    
    def getData(self):
        print(f"Loading from datenum {self.maxId}")
        data = {}



        if(not self.isTest):
            
            parameters = {"datenum":self.maxId, "rownum":0, "type":"global_crude"}

            validResponse = False
            response = {}
            failedAttempts = 0
            
            while not validResponse:
                response = requests.get(str(self.url), auth=HTTPBasicAuth(self.user, self.password), params=parameters)

                if(response.status_code == 200):
                    validResponse = True
                    break

                if(failedAttempts == 10):
                    sendEmail('Error', 'Clipper Data Import', f"Invalid response from Clipper Data, status code: {response.status_code}")
                    break

                failedAttempts += 1

            data = response.json()['record']
        else:
            with open("testData/clipperData.json") as csv_file:
                data = json.load(csv_file)['record']
        
        self.addToCSV(self.csv_file_path, data)

    def run(self):
        self.truncateCSV(f"{self.csv_file_path}\\ClipperData.csv")
        self.truncateCSV(f"{self.csv_file_path}\\ClipperData_Delete.csv")

        self.getData()
        print("done")
