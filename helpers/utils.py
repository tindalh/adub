from pathlib import Path
from dateutil.parser import parse
import datetime
import json
import os
import csv
import pandas as pd
from helpers.log import log

def get_project_root() -> Path:
    """Returns project root folder."""
    return Path(__file__).parent.parent

# string boolean -> int
# Consume a filename and return the date contained in it, or 20000101
def get_date_from_string(string, fuzzy=True):
    try:
        return int(parse(string, fuzzy=fuzzy).strftime('%Y%m%d'))
    except:
        try:
            return int(parse(string.split('_')[-1], fuzzy=fuzzy).strftime('%Y%m%d'))
        except:
            return 20000101

# list(string) -> int
# Consume a list of string and return the most recent date as int (i.e. the largest int)
def get_max_file_date(file_list):
    if(len(file_list) == 0):
        return 20000101
    else:
        first, rest = get_date_from_string(file_list[0]), file_list[1:]

        max_rest = get_max_file_date(rest)

        if(first >= max_rest):
            return first
        else:
            return max_rest

# string datetime.datetime -> String
# Consumes a file path and name and inserts a datestamp
# ASSUME: datestamp is not None
def insert_datestamp_in_filename(file_path, file_name, datestamp):    
    full_file_path = os.path.join(
        file_path, f"{file_name.split('.')[-2]}_{datestamp.strftime('%Y%m%d')}.{file_name.split('.')[-1]}")

    return full_file_path

# string -> JSON
# Consumes a file path and returns the json
def load_json(file_path):
    if(not '.json' in file_path.lower() or not os.path.isfile(file_path)):
        raise ValueError("Must be a JSON file")
    
    with open(file_path) as json_file:
        return json.load(json_file)


# list(dict) string -> None
# Consumes a list of dicts and saves them to a csv file in file_path
def write_to_csv(list_of_dicts, file_path, append=False):
    if(len(list_of_dicts) == 0):
        return

    write_mode = 'w'
    if(append == True):
        write_mode = 'a'

    keys = list_of_dicts[0].keys()
    with open(file_path, write_mode, newline='') as csvfile:
        writer = csv.DictWriter(csvfile, keys, delimiter='|')
        writer.writeheader()
        writer.writerows(list_of_dicts)

# list(string) -> list(string)
# consume a list of strings and return those with a timestamp in the name greater than date
def files_later_than(list_filenames, date):
    if(len(list_filenames) == 0):
        return []
    
    try:
        int_date = int(date.strftime('%Y%m%d'))
    except:
        raise ValueError("Invalid date")

    first, rest = list_filenames[0], list_filenames[1:]
    
    if(get_date_from_string(first) > int_date):
        return [first] + files_later_than(rest, date)
    else:
        return files_later_than(rest, date)


def get_unique_values_for_dataframe_keys(df, keys):
    """
        Pandas.DataFrame list(string) -> dict
        Consumes a dataframe and produces a dict of lists of unique values for the given keys in the dataframe
    """
    d = {}
    for key in keys:
        if(get_date_from_string(str(df[key].iloc[0])) > 20000101 or pd.core.dtypes.common.is_datetime_or_timedelta_dtype(df[key])): # check for valid date
            
            try:
                values = [datetime.datetime.strptime(x, '%Y-%m-%d').strftime('%Y%m%d') for x in df[key].unique()]
            except:
                try:
                    values = [datetime.datetime.strptime(str(x), '%Y%m%d').strftime('%Y%m%d') for x in df[key].unique()]
                except:
                    try:
                        values = [datetime.datetime.strptime(str(x), '%Y-%m-%d').strftime('%Y%m%d') for x in df[key].unique()]
                    except:
                        try:
                            values = [datetime.datetime.strptime(str(x), '%Y-%m-%dT%H:%M:%S.000000000').strftime('%Y%m%d') for x in df[key].unique()]
                        except:
                            try:
                                values = [str(x) for x in df[key].unique()]
                            except:
                                values = []
                                log(__name__, '__integrate__', f"{self.name} has failed for {modified_file_name}: {str(e)}", level="Error")
        else: 
            values = [str(x) for x in df[key].unique()]
        d[key] = values 
    return d