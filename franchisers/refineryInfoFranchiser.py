import sys
import helpers.dataAccess as dtAccss

class RefineryInfoFranchiser(object):
    def __init__(self, server, database):
        self.server = server
        self.database = database
        
    def run(self, unitId):
        dataAccess = dtAccss.DataAccess(self.server, self.database)
        dataAccess.executeStoredProcedure('sp_build_RefineryViews', (int(unitId),))
