import sys
import helpers.dataAccess as dtAccss

class EIADataFranchiser(object):
    def __init__(self, server, database):
        self.server = server
        self.database = database
        
    def run(self, seriesId):
        dataAccess = dtAccss.DataAccess(self.server, self.database)
        dataAccess.executeStoredProcedure('build_mview_EIASeries', (seriesId,))
