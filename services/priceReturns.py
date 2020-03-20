import sys
sys.path.append('..')
from helpers.dataAccess import DataAccess
from helpers.utils import write_to_csv
import os
import numpy
from datetime import datetime, timedelta

## ListOfQuotes ListOfExpiries-> list (list(dicts)) 
## Given a list of quotes, produce the returns curve per tenor

def get_returns_for_curves(loq, loe, is_double_proxy=False):
    """
        ListOfQuotes ListOfExpiries-> list (list(dicts)) 
        Given a list of quotes, produce the returns curve per tenor
    """
    result = []
    curves = get_continuation_curves(loq)

    for i in range(len(curves)):
        m1 = curves[i]
        m2 = None
        shifted_expiry_quotes = []
        if(len(curves) > i + 1):
            shifted_expiry_quotes = get_expiry_quotes(curves[i + 1], loe) 

            if(is_double_proxy):
                m2 = curves[i+1]

        result.append(get_returns_for_curve(m1, shifted_expiry_quotes, m2, is_double_proxy))
        
    return result

## list(Value) list(Value) -> list(Value)
## Given a curve and the expiry quotes from the next curve, build the adjusted curve
## Returns = fn(Quote, PreviousQuote)
## If PreviousQuote is an expiry, use the quote from the curve one further out (i.e. if on M1, use quote from M2)

# def get_returns_for_curve(m1, m2): # stub
#     return m1

# <templated from list(Value), added list(Value) parameter>

def get_returns_for_curve(m1, shifted_expiry_quotes, m2=[], is_double_proxy=False):
    list_returns = []
    for i in range(len(m1)):
        
        previous = { # 1 set up a fallback value
            "Value": 1,
            "Asof":''
        }

        current = m1[i]["Value"]

        if(len(m1) > i + 1): # 2 Try to use the previous days quote
            previous = m1[i + 1]    

        if(is_double_proxy and m1[i]["M"] == 1 ):            
            for v in shifted_expiry_quotes: 
                if(m1[i]["Asof"] == v["Asof"]):        
                    current = m2[i]["Value"]  
                    if(len(m2) > i + 1):
                        previous = m2[i + 1] 
                if(previous["Asof"] == v["Asof"]): # 3 Try to use the shifted quote
                    previous = v 
        else:  
            for v in shifted_expiry_quotes: 
                if(previous["Asof"] == v["Asof"]): # 3 Try to use the shifted quote
                    previous = v

                
                

        m1[i]["Value"] = log_returns(current, previous["Value"])
        
        list_returns.append(m1[i])
    
    return list_returns
    

# float float -> float
# Calculate the log returns between 2 values
def log_returns(v1, v2):
    #return numpy.log(v1/v2)
    return v1 - v2


# list(Value) list(Expiries) -> list(Value)
# Given a curve return only the expiry quotes

# def get_expiry_quotes(lov, loe): # stub
#     return lov

# <templated from ListOfValue, added ListOfExpiry parameter>
def get_expiry_quotes(lov, loe):
    if(lov == []):
        return []
    else:
        
        first = lov[0]
        
        rest = lov[1:]
        for i in range(len(loe)):
            
            if(first['Asof'] == loe[i].ExpiryDate):
                return [first] + get_expiry_quotes(rest, loe[1:])
        return get_expiry_quotes(rest, loe)


# ListOfQuotes -> list(list(dict))
# Consume a list of quotes and produce a continuation curve with instrument name and relative period per contract

# def get_continuation_curves(loq): #stub
#     return list()

# <template from ListOfQuotes, added ListOfExpiries parameter>

def get_continuation_curves(loq):
    ccurves = []
    for rp in list({quote.RelativePeriod for quote in loq}):
        ccurves.append(get_continuation_curve(rp, loq))
    return ccurves


# int ListOfQuotes -> list(dict)
# Given a list of quotes, return the series for the given relative period

# def get_continuation_curve(rp, loq): # stub
#     return []

# <template from ListOfQuotes, added atomic parameter>

def get_continuation_curve(rp, loq):
    result = []
    for q in loq:
       
        if(is_series_quote(rp, q)):
            result.append(get_as_series_item(rp, q)[0])

    return result


# int Quote -> Boolean
# Given a quote and a relative period, return true if the quote is the same relative period:
def is_series_quote(rp, q):
    if(q.RelativePeriod == rp):
        return True
    return False


# int Quote -> list(dict)
# Create a dictionary of instrument name, contract and relative period from given quote
def get_as_series_item(rp, q): # TODO
    sitem = {
        'Instrument': q.IdInstrument,
        'Asof': q.Asof,
        'M': rp,
        'Value': q.Value
    }
    return [sitem]


# ListOfExpiries -> ListOfExpiries
# Sort a list of expiries by contract date
def sort_expiries(loe, reverse=False): # stub
    return sorted(loe, key=lambda k: k.Contract, reverse=reverse)


# ListOfQuotes -> ListOfQuotes
# Sort a list of quotes by asof then relative period date
def sort_expiries(loe, reverse=False): # stub
    return sorted(loe, key=lambda k: k.Contract, reverse=reverse)

def generate_returns():
    dtAccss = DataAccess(os.environ['ADUB_DBServer'], 'Price')

    traded_instruments =  dtAccss.load('view_TradedInstruments')

    for i in traded_instruments:  
        returns_filter = {'IdInstrument': [i.Id]}
        most_recent_returns = dtAccss.get_max_database_date('view_Returns', 'Asof', 'dbo', **returns_filter)

        quotes_filter = {'IdInstrument': i.Id, '>=Asof': datetime.strftime(most_recent_returns, "%Y-%m-%d")} 
        quotes = dtAccss.load('view_Quotes_Futures', **quotes_filter)
        quotes.reverse()

        expiries_filter = {'IdInstrument': i.Id}
        expiries = dtAccss.load('Expiry', **expiries_filter)
        
        if (len(expiries) == 0):
            expiries_filter = {'IdSource': i.IdSource}
            expiries = dtAccss.load('Expiry', **expiries_filter)

        result = get_returns_for_curves(quotes, expiries, i.IsDoubleProxyRoll)

        combined_result = []
        for tenor in result:
            combined_result.extend(tenor[:-1])

        if(len(combined_result) > 0):
            file_name = f"{os.environ['ADUB_Import_Output_UNC']}\\Price\\Returns\\{i.Name.replace(' ','_').replace('/','_')}_returns.csv"
            write_to_csv(combined_result, file_name)

            delete_filter = {'IdInstrument': [i.Id], '>Asof': [datetime.strftime(most_recent_returns, "%Y-%m-%d")]} 
            dtAccss.delete('returns', **delete_filter)
            dtAccss.bulkInsert('returns', file_name)


if(__name__ == "__main__"):
    generate_returns()



    
        
