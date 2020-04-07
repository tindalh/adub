import csv
import os
import datetime
import requests
import sys
sys.path.append('..')
import helpers.dataAccess as dtAccss
import helpers.stringHelper as strngHlpr
import json
from helpers.log import log


class EiaImporter(object):
    def __init__(self, server, database, url, api_key, file_path, bulkinsert_path):
        self.api_key = api_key       
        self.server = server
        self.database = database 
        self.url = url
        self.file_path = file_path
        self.bulkinsert_path = bulkinsert_path 
        self.series = list()
        self.testLevel = 0

    def get(self, action, parameters, testLevel=0, testType='category'):
        if(not self.url == 'test'):
            log(__name__, 'get', f"loading {action}...")
            response = requests.get(str(self.url) + '/' + str(action) + '/', parameters)
            return(response.json())
        else:
            if(action == 'series' and self.testLevel == 2):
                log(__name__, 'get', f"loading test series...'")
                with open('tests/data/series.json') as json_file:
                    data = json.load(json_file)
                    self.testLevel += 1
                    return data 
            elif(testLevel == 0):
                log(__name__, 'get', f"loading test parent category...'")
                with open('tests/data/category.json') as json_file:
                    data = json.load(json_file)
                    self.testLevel += 1
                    return data 
            elif(testLevel == 1):
                if(testType == 'category'):
                    log(__name__, 'get', f"loading test child category...'")
                    with open('tests/data/childCategory.json') as json_file:
                        data = json.load(json_file)
                        self.testLevel += 1
                        return data 

    def truncateCSV(self, file_path):
        f = open(file_path, "w+")
        f.close()

    def truncateAll(self):
        self.truncateCSV('{}\\category.csv'.format(self.file_path))
        self.truncateCSV('{}\\series.csv'.format(self.file_path))
        self.truncateCSV('{}\\categorySeries.csv'.format(self.file_path))
        self.truncateCSV('{}\\seriesData.csv'.format(self.file_path))

    def writeDictToCSV(self, dict, file_path):
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=dict.keys(), delimiter='|', quoting=csv.QUOTE_MINIMAL)
            
            writer.writerow(dict)
    
    def writeToCSV(self, rows, file_path):
        with open(file_path, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter='|', quoting=csv.QUOTE_MINIMAL)
        
            for row in rows:
                writer.writerow(row)

    def setParameters(self, queryType, id):
        if(id == None):
            raise ValueError("Id should have a value")
        if(queryType.lower() == 'category'):
            return {'category_id':id,"api_key":self.api_key}
        elif(queryType.lower() == 'series'):
            return {'series_id':id,"api_key":self.api_key}
        else:
            raise ValueError("Query type {} not implemented yet".format(queryType))

    def getSeriesString(self, series, isSQL=False):
        """
            Assumes series is a list of series dicts
            Returns a ; joined string of up to 100 series ids 
        """
        countSeries = 0
        stringSeriesIds = ''
        for i in range(len(series)):     
            
            countSeries += 1
            if(not isSQL):
                stringSeriesIds += series[i]['series_id'] + ';'
            else:
                stringSeriesIds += "'" + series[i]['series_id'] + "',"

            if(countSeries == 100 or i == len(series) - 1):
                yield stringSeriesIds[:-1]

                countSeries = 0     
                stringSeriesIds = ''

    def addRootCategoryIfNotExists(self, id, dataAccess):
        if(not dataAccess.exists('EIACategory','category_id',id)):
            dataAccess.executeRawSQL("insert into EIACategory (category_id, name) values (371, 'EIA Data Sets')")

    def isNewCategory(self, category_id, dataAccess):
        return not dataAccess.exists('EIACategory','category_id',category_id)

    def isUpdatedSeries(self, series_id, updated, dataAccess):
        return not dataAccess.exists('EIASeries','series_id', "'" + series_id + "'", "AND updated = '{}'".format(updated[:10]))

    def isNewSeries(self, series_id, dataAccess):
        exists = dataAccess.exists('EIASeries','series_id', "'" + series_id + "'")
        return not exists

    def isNewCategorySeries(self, series_id, category_id, dataAccess):
        return not dataAccess.exists('EIACategorySeries','series_id', "'" + series_id + "'", "AND category_id = {}".format(category_id))

    def deleteFromDb(self, series_id, dataAccess):
        log(__name__, 'deleteFromDb', f"Deleting {series_id}")

        dataAccess.deleteById('EIASeries', 'series_id',  "'" + series_id + "'")
        dataAccess.deleteById('EIASeriesData', 'series_id',  "'" + series_id + "'")
    
    def saveSeries(self, series):
        data = {key: value for key, value in series.items() if key not in ['data','unitsshort','iso3166','latlon','latlon2','lat','lon','description','copyright','geography','geography2','lastHistoricalPeriod']}
        data['updated']=data['updated'][:10]
        
        self.writeDictToCSV(data, '{}\\series.csv'.format(self.file_path))   

    def saveCategory(self, category):
        log(__name__, 'saveCategory', f"Saving category {category['category_id']}")
        data = {key: value for key, value in category.items() if key not in ['childcategories','childseries']}
        self.writeDictToCSV(data, '{}\\category.csv'.format(self.file_path))

    def saveCategorySeries(self, category_id, series_id):
        data = {'category_id':category_id, 'series_id': series_id}
        self.writeDictToCSV(data, '{}\\categorySeries.csv'.format(self.file_path))

    def run(self, category_id):
        self.truncateAll()
        dataAccess = dtAccss.DataAccess(self.server, self.database)
        self.addRootCategoryIfNotExists(371, dataAccess)
        self.loadCategory(category_id, dataAccess)
        self.save()

        dataAccess.executeStoredProcedure('build_mview_EIASeries', ())

    def runSeries(self, series_id='all'):
                
        self.truncateAll()
        dataAccess = dtAccss.DataAccess(self.server, self.database)
        if(not type(series_id) is str):
            series_id = series_id.decode()

        if(series_id == 'all'):
            log(__name__, 'runSeries', f"Updating all series")
            seriesToUpdate = dataAccess.load('EIASeriesShortname')

            listOfSeriesDicts = [{"series_id": x[1]} for x in seriesToUpdate]
            reportString = strngHlpr.htmlify([(x[0], x[1]) for x in seriesToUpdate])
            
            for s in self.getSeriesString(listOfSeriesDicts):
                self.loadSeries(s, None, dataAccess)
                self.save()
                
                for id in s.split(';'):
                    dataAccess.executeStoredProcedure('build_mview_EIASeries', (str(id),))
        else:
            reportString = strngHlpr.htmlify([series_id])
            self.loadSeries(series_id, None, dataAccess)
            self.save()
            
            for id in series_id.split(';'):
                    dataAccess.executeStoredProcedure('build_mview_EIASeries', (str(id),))

        log(__name__, 'runSeries', f"{len(seriesToUpdate)} series were updated.", email=True, emailSubject='Eia Update', emailTable=reportString)
        
    def loadCategory(self, category_id, dataAccess, isRoot=False):
        parameters = self.setParameters('category', category_id)
        
        try:
            category = self.get('category', parameters, testLevel=self.testLevel)['category']
        except:
            raise ValueError('Category not found')       
        

        if(self.isNewCategory(category['category_id'], dataAccess) == True):
            self.saveCategory(category)  

        if(isRoot): # only loading the root category so don't want to recursively get all children
            return

        childCategories = category.get('childcategories', [])

        log(__name__, 'loadCategory', f"Processing category {category_id} with {len(childCategories)} child categories")
        
        # Load all the child categories
        for c in childCategories:
            self.loadCategory(c['category_id'], dataAccess)

        # Load all the child series
        for s in self.getSeriesString(category.get('childseries',[])) :
            self.loadSeries(s, category['category_id'], dataAccess)
        

    def loadSeries(self, series_id, category_id, dataAccess):
        
        parameters = self.setParameters('series', series_id)

        try:
            listSeries = self.get('series', parameters)['series']
            log(__name__, 'loadSeries', f"{len(listSeries)} series retrieved from Eia")

            for s in listSeries:
                
                if(s['series_id'] not in self.series):
                    if(self.isNewSeries(s['series_id'], dataAccess)): # it doesn't exist in the database

                        self.series.append(s['series_id'])
                        self.saveSeries(s)
                        self.loadData(s)

                    elif(self.isUpdatedSeries(s['series_id'], s['updated'], dataAccess)):  # it exists in the database an needs updating
                        
                        self.series.append(s['series_id'])
                        self.saveSeries(s)
                        self.loadData(s)
                        self.deleteFromDb(s['series_id'], dataAccess)
                
                if(category_id is not None):
                    if(self.isNewCategorySeries(s['series_id'], category_id, dataAccess)):
                        self.saveCategorySeries(category_id, s['series_id'])      

        except Exception as e:
            if(self.url == 'test'):
                #dont worry
                log(__name__, 'loadSeries', f"{str(e)}")
            else:
                log(__name__, 'loadSeries', f"Error title: {str(e)}. Series not found: {series_id}:", level='Error', email=True, emailSubject='Eia Update')
                     
            
            
    def loadData(self, series):
        for i in range(len(series['data'])):
            series['data'][i].insert(0, series['series_id'])

        self.writeToCSV(series['data'], '{}\\seriesData.csv'.format(self.file_path))

    def save(self):
        log(__name__, 'loadSeries', f"Saving from CSV's in {self.bulkinsert_path}")

        dataAccess = dtAccss.DataAccess(self.server, self.database)

        self.bulkInsert('series', '{}\\series.csv'.format(self.bulkinsert_path), dataAccess)
        self.bulkInsert('seriesData', '{}\\seriesData.csv'.format(self.bulkinsert_path), dataAccess)        
        self.bulkInsert('category', '{}\\category.csv'.format(self.bulkinsert_path), dataAccess)        
        self.bulkInsert('categorySeries', '{}\\categorySeries.csv'.format(self.bulkinsert_path), dataAccess)            

    def bulkInsert(self, table, path, dataAccess):
        log(__name__, 'loadSeries', f"Inserting {table} from {path}")

        if(table == 'seriesData'):
            sql = """
                
                SELECT top 0 series_id,dateString,value into #EIASeriesData from EIASeriesData;
                BULK INSERT #EIASeriesData FROM '{}' WITH (FIELDTERMINATOR='|');
                Insert INto EIASeriesData (series_id,dateString,value) Select series_id,dateString,value from #EIASeriesData
            """
        elif(table == 'series'):
            sql = """
                SELECT top 0 series_id,name,units,f,source,[start],[end],updated into #EIASeries from EIASeries;
                BULK INSERT #EIASeries FROM '{}' WITH (FIELDTERMINATOR='|');
                Insert INto EIASeries (series_id,name,units,f,source,[start],[end],updated) Select series_id,name,units,f,source,[start],[end],updated from #EIASeries
            """
        elif(table == 'category'):
            sql = """
                SELECT top 0 category_id,parent_category_id,name, notes into #EIACategory from EIACategory;
                BULK INSERT #EIACategory FROM '{}' WITH (FIELDTERMINATOR='|');
                Insert INto EIACategory (category_id,parent_category_id,name, notes) Select category_id,parent_category_id,name, notes from #EIACategory
            """

        elif(table == 'categorySeries'):
            sql = """
                SELECT top 0 category_id, series_id into #EIAcategorySeries from EIAcategorySeries;
                BULK INSERT #EIAcategorySeries FROM '{}' WITH (FIELDTERMINATOR='|');
                Insert INto EIAcategorySeries (category_id, series_id) Select category_id, series_id from #EIAcategorySeries
            """
        dataAccess.executeRawSQL(sql.format(path))
