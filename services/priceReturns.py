from helpers.dataAccess import DataAccess

import os

## Data Definitions
## Return is { 
#   date: float
# }

rtrn1 = {
    '2019-01-01': 2.3021
}

rtrn2 = {
    '2019-01-02': 1.9821
}

## ExpiryReturns is one of
## - None
## - list(Return)

er0 = list()
er1 = [rtrn1,rtrn2]

## ListOfExpiryReturns is one of
## - None
## - list(ExpiryReturns)

loer0 = list()
loer1 = [er0,er1]

def calculate_returns(instrument_name):
    """
        Instrument -> list(dict)
        Consumes an instrument name and produces a list of expiries with a list of returns
    """
    dtAccss = DataAccess('arcsql', 'Price')
    
    d = {'Instrument': instrument_name}
    return dtAccss.load('view_Quotes_Futures', **d)