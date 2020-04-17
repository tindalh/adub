import sys
sys.path.append('..')

from services.excelExtractor import extract_files
import os
import csv
import json
import datetime

from helpers.dataAccess import DataAccess
from helpers.utils import list_to_csv
from helpers.log import error_email, log

def import_energy_aspect_runs(file_path):
    try:
        d = None
        with open("../templates/global.json") as file:
            data = json.load(file)
            d = extract_files([file_path], data)
            
        result = []
        for dataset in d:
            rows = dataset["rows"]
            name = dataset["name"]

            cs = list(rows[0].keys())
            uploaded = datetime.datetime.now().strftime("%Y-%m-%d")
            
            result.extend(pivot(cs[1:], cs[0], rows, uploaded))

        persist(result, 'EnergyAspects', "EA_Runs", 'RefineryInfo', "import", 'EnergyAspects_Runs', "Uploaded", 0)

        log("Energy Aspects", "import_energy_aspect_runs", "The job has completed")
    except Exception as e:
        error_email("Energy Aspects", "import_energy_aspect_runs", str(e) )

def persist(rows, directory, file_name, database, schema, db_table_name, date_column, date_column_index):
    data_access = DataAccess(os.environ['ADUB_DBServer'], database)  
    output_directory = os.path.join(os.environ['ADUB_Import_Output_UNC'], directory)
    
    list_to_csv(rows, output_directory + '\\' + file_name + '.csv')
    if(len(rows) > 0):
        d = {
            '>=' + date_column : [str(min(rows, key=lambda x: x[1])[date_column_index])]
        }

        data_access.delete(schema + '.' + db_table_name, **d)
        data_access.bulkInsert(schema + '.' + db_table_name,  output_directory + '\\' + file_name + '.csv', first_row=1)


def pivot(cs, pivot_around, rs, uploaded=None):
    if(len(cs) == 0):
        return []
    else:
        first, rest = cs[0], cs[1:]

        if (uploaded is not None):
            return [(uploaded,r[pivot_around],first,r[first])  for r in rs] + pivot(rest, pivot_around, rs, uploaded)
        else:
            return [(r[pivot_around],first,r[first])  for r in rs] + pivot(rest, pivot_around, rs, uploaded)


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

if(__name__ == "__main__"):
    import_energy_aspect_runs("C:/dev/temp/EnergyAspects/ea_runs.xlsx")

    