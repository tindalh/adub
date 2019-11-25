import unittest
import sys
sys.path.append("..")
from dataAccess import DataAccess

class TestDataAccess(unittest.TestCase):
    def setUp(self):
        self.server = "Lon-PC53"
        self.database = "Analytics"
        
    # python -m unittest test_dataAccess.TestDataAccess.test_loadToCSV
    def test_loadToCSV(self):
        dataAccess = DataAccess(self.server, self.database)
        self.assertIsNone(dataAccess.loadToCSV("Select * from Period"))

    # python -m unittest test_dataAccess.TestDataAccess.test_bulkInsert
    def test_bulkInsert(self):
        dataAccess = DataAccess(self.server, 'IEAData')
        self.assertIsNone(dataAccess.bulkInsert('SUPPLY', "C:\Dev\Excel Files\Output\IEA\Supply.txt", truncate=True ))

    # python -m unittest test_dataAccess.TestDataAccess.test_deleteById
    def test_deleteById(self):
        dataAccess = DataAccess(self.server, 'Analytics')
        self.assertIsNone(dataAccess.deleteById('RystadProduction', "Period", "'2019-02-01'" ))

    # python -m unittest test_dataAccess.TestDataAccess.test_bulkInsert_withDelete
    def test_bulkInsert_withDelete(self):
        dataAccess = DataAccess(self.server, 'Analytics')
        self.assertIsNone(dataAccess.bulkInsert('bview_RystadProduction', "C:\Dev\Excel Files\Output\RystadProduction\Rystad Production_2013.csv", truncate=False, Period="2019-01-01" ))

    # python -m unittest test_dataAccess.TestDataAccess.test_load
    def test_load(self):
        dataAccess = DataAccess(self.server, self.database)
        self.assertIsInstance(dataAccess.load('Commodity', Name = "Crude"), list)
        self.assertIsInstance(dataAccess.load('Commodity'), list)
        self.assertIsInstance(dataAccess.load('Commodity', Name = "Crude", Description=None), list) # TODO where is null
