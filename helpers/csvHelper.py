import os
import pandas as pd
from helpers.log import log as log

def getDataframe(file, delimiter=None, names=None, newline=''):
    """
        Consumes the file name with path for a csv file and returns a data frame
    """
    try:
        if(file is not None):
            log(__name__, 'getDataframe', f"Reading from {file}")
            return pd.read_csv(file, sep=delimiter, names=names, encoding = "ISO-8859-1")
    except FileNotFoundError as f:
        log(__name__, 'getDataframe', f"The file {file} doesn't exist", 'Error', True, 'CsvImporter')
        raise FileNotFoundError
    except Exception as e:
        log(__name__, 'getDataframe', f"Failed to process the file: {str(e)}", 'Error', True, 'CsvImporter')

