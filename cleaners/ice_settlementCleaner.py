import pandas as pd
import datetime
from dateutil.parser import parse

import sys
sys.path.append("..")
from helpers.log import log

def _parse_contract_column(x):
    dt = parse(x)
    dt_first_day = dt.replace(day=1)
    return dt_first_day.strftime('%Y%m%d')

def _set_asof(df, datestamp):
    
    df['Asof'] = datestamp.strftime('%Y%m%d')
            
    return df

def _format_contract_column(df):   
    
    df['ContractDate'] = df['ContractDate'].apply(_parse_contract_column)
            
    return df

def _set_curve_name_column(df, name):   
    
    df['Curve'] = name
            
    return df

def _arrange_columns(df):
    
    cols = df.columns
    cols = [cols[3], cols[2], cols[0], cols[1]]
    return df[cols]


def clean(df, datestamp, curve_name):
    log(__name__, 'clean', f"Cleaning data")

    dfResult = df
    dfResult = _set_asof(dfResult, datetime.datetime.strptime(str(datestamp), '%Y%m%d'))
    dfResult = _format_contract_column(dfResult)
    dfResult = _set_curve_name_column(dfResult, curve_name)
    dfResult = _arrange_columns(dfResult)
    return dfResult