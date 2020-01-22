import pandas as pd
import datetime

import sys
sys.path.append("..")
from helpers.log import log

def _replaceInvalidQuantities(df):
    dfReplaced = df
    if('Quantity' in df.columns):  
        df['Quantity'].replace(["x"], '0',inplace=True)
    if('VALUE' in df.columns):  
        df['VALUE'].replace(["x"], '0',inplace=True)

    return dfReplaced


def _cleanDateColumns(df):
    """
        Dataframe -> Dataframe
        Consumes a dataframe and converts any date like columns to dtype.datetime64[ns]
    """
    if('Period' in df.columns.tolist()):
        df['Period'] = pd.to_datetime(df['Period'], format='%Y-%m-%d', errors='ignore', infer_datetime_format=True)
    elif('TIMESTAMP' in df.columns.tolist()):
        df['Period'] = pd.to_datetime(df['TIMESTAMP'], format='%Y-%m-%d', errors='ignore', infer_datetime_format=True)

    return df


def _setFrequency(df):
    """
        Dataframe -> Dataframe
        Consumes a dataframe and converts any frequency to one of Day, Month, Quarter, Year
    """
    if('Period' in df.columns.tolist()):
        df.loc[df['Period'].str.contains('Q'), 'PeriodType'] = 'Quarter'
        df.loc[df['Period'].str.len() == 4, 'PeriodType'] = 'Year'
        df.loc[df['Period'].str.len() == 7, 'PeriodType'] = 'Month'
    elif('FREQUENCY' in df.columns.tolist()):
        df.loc[df['FREQUENCY'].str.contains('quarterly'), 'PeriodType'] = 'Quarter'
        df.loc[df['FREQUENCY'].str.contains('yearly'), 'PeriodType'] = 'Year'
        df.loc[df['FREQUENCY'].str.contains('monthly'), 'PeriodType'] = 'Month'    
    return df

def _setAsof(df):
    """
        Dataframe -> Dataframe
        Consumes a dataframe and converts any frequency to one of Day, Month, Quarter, Year
    """
    
    df['Asof'] = datetime.date.today()
            
    return df

def _remove_header_row(df):
    if(df[df.columns[0]].iloc[0] == df.columns[0]):
        df = df.drop([0])
    return df


def clean(df):
    log(__name__, 'clean', f"Cleaning data")

    dfResult = df
    dfResult = _replaceInvalidQuantities(dfResult)
    dfResult = _setFrequency(dfResult)
    dfResult = _setAsof(dfResult)
    dfResult = _cleanDateColumns(dfResult)
    dfResult = _remove_header_row(dfResult)
    return dfResult