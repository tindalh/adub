import unittest
import sys
import os
sys.path.append('..')
import importers.eiaImporter as eia
import helpers.dataAccess as dtAccss


class TestEiaImporter(unittest.TestCase):
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

    
    # python -m unittest test_EiaImporter.TestEiaImporter.test_runSeries
    def test_runSeries(self):              
        self.assertIsNone(self.eiaImporter.runSeries(self.seriesString))

    def test_runSeries_fromEiaSeriesShortname(self):              
        self.assertIsNone(self.eiaImporter.runSeries())

    def test_loadSeries(self):
        self.assertIsNone(self.eiaImporter.loadSeries(self.seriesString, None, self.dataAccess))

if(__name__ == '__main__'):
    unittest.main()