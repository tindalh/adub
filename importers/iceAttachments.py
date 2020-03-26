import sys
sys.path.append('..')
import os
import csv
import datetime

from helpers.dataAccess import DataAccess
from helpers.utils import get_date_from_string

def import_ICE_attachments(file_parts):

    for curve in file_parts:
        file_name = curve[4:]
        schema = 'import'
        db_table_name = 'ICE_Settlement_Curve'
        root_directory = os.path.join(os.environ['ADUB_Import_Path'], 'ICE_Settlement')
        curve_part = file_name

        max_imported = get_max_date_imported(os.environ['ADUB_DBServer'], 'Price', db_table_name, [curve])

        data = import_from_directory(os.path.join(root_directory, curve_part), max_imported)
        export_to_csv(data, os.path.join(root_directory, file_name) + '.csv')

        if(len(data) > 0):
            data_access = DataAccess(os.environ['ADUB_DBServer'], 'Price')

            d = {
                'Curve' : [data[0][0]],
                '>=Asof' : [str(min(data, key=lambda x: x[1])[1])]
            }

            data_access.delete(schema + '.' + db_table_name, **d)
            data_access.bulkInsert(schema + '.' + db_table_name, os.path.join(root_directory, file_name) + '.csv', first_row=1)
            data_access.executeStoredProcedure('sp_load_ice_settlement_prices_current', [curve])

def get_max_date_imported(server, database, table, curves):
    data_access = DataAccess(server, database)  

    max_database_date = datetime.datetime.now()
    for curve in curves:
        d = {'Curve':[curve[4:]]}

        max_database_date = min(max_database_date, data_access.get_max_database_date(table, 'Asof', schema_name='import', **d))
    
    return max_database_date

# String -> List
def import_from_directory(directory, max_saved):
    list_data = []
    for filename in os.listdir(directory):
        if(datetime.datetime.strptime(str(get_date_from_string(filename)), '%Y%m%d') > max_saved):
            with open(os.path.join(directory, filename)) as f:
                reader = csv.reader(f)
                data = get_data_list(list(reader), directory, filename)
                list_data.extend(data)
    return list_data

def get_data_list(rows, directory, filename):
    data = []
    for i in rows:
        if(len(i[0]) > 0 and i[0].lower() != 'strip'):
            data.append([os.path.basename(directory)] + [get_date_from_string(filename)] + [parse_contract_column(i[0])] + [i[1]])
    return data



def parse_contract_column(date):
    dt = datetime.datetime.strptime(date, '%b%y')
    dt_first_day = dt.replace(day=1)
    return dt_first_day.strftime('%Y%m%d')


# List -> CSV
def export_to_csv(list_data, file_name):
    with open(file_name, 'w', newline='') as f:
        writer = csv.writer(f, delimiter='|')
        writer.writerows(list_data)

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