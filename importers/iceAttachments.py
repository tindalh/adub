import sys
sys.path.append('..')
import os
import csv
import datetime

from helpers.dataAccess import DataAccess
from helpers.utils import get_date_from_string, list_to_csv, get_list_from_directory_csv_files

def import_ICE_attachments(file_parts):
    data_access = DataAccess(os.environ['ADUB_DBServer'], 'Price')  

    for curve in file_parts:
        file_name = curve
        schema = 'import'
        db_table_name = 'ICE_Settlement_Curve'
        root_directory = os.path.join(os.environ['ADUB_Import_Path'], 'ICE_Settlement')

        output_directory = os.path.join(os.environ['ADUB_Import_Output_UNC'], 'ICE_Settlement', file_name)
        curve_part = file_name

        max_imported = get_max_date_imported(data_access, db_table_name, [curve])

        data = get_list_from_directory_csv_files(os.path.join(root_directory, curve_part), max_imported, get_data_list)
        list_to_csv(data, output_directory + '\\' + file_name + '.csv')

        if(len(data) > 0):
            d = {
                'Curve' : [data[0][0]],
                '>=Asof' : [str(min(data, key=lambda x: x[1])[1])]
            }

            data_access.delete(schema + '.' + db_table_name, **d)
            data_access.bulkInsert(schema + '.' + db_table_name,  output_directory + '\\' + file_name + '.csv', first_row=1)
            data_access.executeStoredProcedure('sp_load_ice_settlement_prices_current', [curve])


def get_max_date_imported(data_access, table, curves):
    """
        DataAccess str list(str) -> datetime
        Gets the max date in the database for the given curve names
    """

    max_database_date = datetime.datetime.now()
    for curve in curves:
        d = {'Curve':[curve]}

        max_database_date = min(max_database_date, data_access.get_max_database_date(table, 'Asof', schema_name='import', **d))
    
    return max_database_date


def get_data_list(rows, directory, file_date_str):
    """
        list(list) str str -> list(list)
        Adds columns to a list of rows
    """
    data = []
    for i in rows:
        if(len(i[0]) > 0 and i[0].lower() != 'strip'):
            data.append([os.path.basename(directory)] + [file_date_str] + [parse_contract_column(i[0])] + [i[1]])
    return data


def parse_contract_column(str_date):
    """
        str -> str
        Given a date str in format MMMYY, produce a str in YYYYMMDD
    """
    dt = datetime.datetime.strptime(str_date, '%b%y')
    dt_first_day = dt.replace(day=1)
    return dt_first_day.strftime('%Y%m%d')


if(__name__ == "__main__"):   
    curves = [
        'ICE 1630 SGT Brent Crude Futures', 
        'ICE 1630 SGT LS Gas Oil Futures', 
        'ICE 1930 LS Gas Oil Futures',
        'ICE 1630 WTI Crude Futures',
        'ICE 1630 Heating Oil Futures',
        'ICE 1630 (RBOB) Gasoline Futures',
        'ICE 1630 Brent Crude Futures'
    ]

    import_ICE_attachments(curves)