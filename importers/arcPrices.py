import subprocess
import sys
import os
import argparse
sys.path.append('..')
from helpers.utils import get_project_root
from helpers.dataAccess import DataAccess
from services.priceReturns import generate_returns
from helpers.log import log
from constants import PRICE_DB_NAME

# Constant
ROOT_DIR = get_project_root()


# Changing
current_table_names = [
    {
        "Source": "TEMP_LANDED_MFL_L1_ICE_1",
        "Target": "ICE_L1_1_Current"
    },
    {
        "Source": "TEMP_LANDED_MFL_L1_ICE_2",
        "Target": "ICE_L1_2_Current"
    },{
        "Source": "TEMP_LANDED_MFL_L1_ICE_3",
        "Target": "ICE_L1_3_Current"
    },{
        "Source": "view_TEMP_LANDED_MFL_L1_ICE_5",
        "Target": "ICE_L1_5_Current"
    },
    {
        "Source": "TEMP_LANDED_MFL_L1E_ICE_1",
        "Target": "ICE_L1E_1_Current"
    },
    {
        "Source": "TEMP_LANDED_MFL2_L1_ICE_1",
        "Target": "ICE_MFL2_L1_1_Current"
    },
    {
        "Source": "TEMP_LANDED_MFL_L2_CME_1",
        "Target": "Nymex_L2_1_Current"
    },
    {
        "Source": "TEMP_LANDED_MFL_L2_CME_2",
        "Target": "Nymex_L2_2_Current"
    },
    {
        "Source": "TEMP_LANDED_MFL_L4_PLT_1",
        "Target": "Platts_L4_1_Current"
    }
]

# Changing
history_table_names = [
    {
        "Source": "WRK_VAULT_MFL_L1_ICE_1",
        "Target": "ICE_L1_1_History"
    },
    {
        "Source": "WRK_VAULT_MFL_L1_ICE_2",
        "Target": "ICE_L1_2_History"
    },{
        "Source": "WRK_VAULT_MFL_L1_ICE_3",
        "Target": "ICE_L1_3_History"
    },
    {
        "Source": "view_WRK_VAULT_MFL_L1_ICE_5",
        "Target": "ICE_L1_5_History"
    },
    {
        "Source": "WRK_VAULT_MFL_L1E_ICE_1",
        "Target": "ICE_L1E_1_History"
    },
    {
        "Source": "WRK_VAULT_MFL2_L1_ICE_1",
        "Target": "ICE_MFL2_L1_1_History"
    },
    {
        "Source": "WRK_VAULT_MFL_L2_CME_1",
        "Target": "Nymex_L2_1_History"
    },
    {
        "Source": "WRK_VAULT_MFL_L2_CME_2",
        "Target": "Nymex_L2_2_History"
    },
    {
        "Source": "WRK_VAULT_MFL_L4_PLT_1",
        "Target": "Platts_L4_1_History"
    }
]

def bulk_copy(table_names, env_dict):

    print(
        'Target server: ' + env_dict["target_server"] + \
        '\nSource Server: ' + env_dict["source_server"] + \
        '\nFile path: ' + env_dict["file_path"])
    
    for table in table_names:
        source_table_name = table["Source"]
        target_table_name = table["Target"]

        try:
            format_file_path = os.path.join(ROOT_DIR, "format",
                                f"format.{source_table_name}.fmt")

            subprocess.run(  # bcp to a file
                [
                    # the batch file
                    os.path.join(ROOT_DIR, "batch", "bcp.out.file.bat"),
                    env_dict["source_db_schema"] + '.' + source_table_name,
                    os.path.join(env_dict["file_path"], 'data.' + source_table_name) + ".txt",
                    format_file_path,
                    env_dict["source_server"]
                ], check=True
            )

            subprocess.run(  # truncate target table
                [
                    # the batch file
                    os.path.join(ROOT_DIR, "batch", "sqlcmd.truncate.bat"),
                    env_dict["target_server"],
                    env_dict["target_db_schema"] + '.' + target_table_name
                ], check=True
            )

            subprocess.run(  # bcp from a file
                [
                    # the batch file
                    os.path.join(ROOT_DIR, "batch", "bcp.in.file.bat"),
                    env_dict["target_db_schema"] + '.' + target_table_name,
                    os.path.join(env_dict["file_path"], 'data.' + source_table_name) + ".txt",
                    os.path.join(ROOT_DIR, "format",
                                f"format.{source_table_name}.fmt"),
                    env_dict["target_server"],
                ], check=True
            )
        except Exception as cpe:
            print(str(cpe))

def get_table_names(data_type):
    if(data_type.lower().strip() == 'current'):
        return current_table_names
    else:
        return history_table_names

def get_env_dict(env_name, remote=False):
    d = {}

    if ('dev' in env_name.lower().strip()):
        d["source_server"] = "ARCBO8\\DEV"
        d["target_server"] = "Lon-PC53"
        d["source_db_schema"] = "ARC_DOCDROP.dbo"
        d["target_db_schema"] = "Price.import"
        d["file_path"] = "c:\\temp"


    elif ('test' in env_name.lower().strip()):
        d["source_server"] = "ARCBO8\\DEV"
        d["target_server"] = 'arcsql.arcpet.co.uk\\mssqlserverdev'
        d["source_db_schema"] = "ARC_DOCDROP.dbo"
        d["target_db_schema"] = "Price.import"
        if(remote):
            d["file_path"] = "\\\\arcsql\\RefineryInfo\\RefineryUpload\\Test\\ArcPrices"
        else:
            d["file_path"] = "d:\\temp"

    elif ('prod' in env_name.lower().strip()):
        d["source_server"] = "ARCBO3\\LIVE"
        d["target_server"] = 'arcsql.arcpet.co.uk'
        d["source_db_schema"] = "ARC_DOCDROP.dbo"
        d["target_db_schema"] = "Price.import"
        if(remote):
            d["file_path"] = "\\\\arcsql\\RefineryInfo\\RefineryUpload\\ArcPrices"
        else:
            d["file_path"] = "d:\\temp"

    else: 
        raise ValueError("Environment arg should be one of dev, test, prod")

    return d

def run_daily_prices():
    try:
        
        table_names = get_table_names('current')

        env = 'prod'
        if('dev' in os.environ['ADUB_DBServer'].lower()):
            env = 'test'
        elif('pc53' in os.environ['ADUB_DBServer'].lower()):
            env = 'dev'
        elif('desktop' in os.environ['ADUB_DBServer'].lower()):
            env = 'dev'

        log(__name__, 'run_daily_prices', f"Running import of prices in {env}")
        
        env_dict = get_env_dict(env)

        # get raw data
        bulk_copy(table_names, env_dict)

        log(__name__, 'run_daily_prices', f"Finished bulk copy")

        # process prices
        dta_accss = DataAccess(os.environ['ADUB_DBServer'], 'Price')
        dta_accss.executeStoredProcedure('sp_load_ice_prices_current')
        dta_accss.executeStoredProcedure('sp_load_nymex_prices_current')
        dta_accss.executeStoredProcedure('sp_load_platts_prices_current')
        log(__name__, 'run_daily_prices', f"Finished stored proc")

        generate_returns()

        log(__name__, 'run_daily_prices', f"Finished generating returns")
    except Exception as e:
        log(__name__, 'run_daily_prices', f"Arc Price Importer has failed: {str(e)}", level="Error", email=True, emailSubject='Arc Price Importer')


if(__name__ == "__main__"):
    parser = argparse.ArgumentParser(description='Import prices from ARC_DOCDROP.dbo')

    parser.add_argument('--data', help='current day or history', default='current')
    parser.add_argument('--env', help='dev, test, prod', default='dev')
    parser.add_argument('--remote', help='executing from remote machine', default=False)
    args = parser.parse_args()

    table_names = get_table_names(args.data)
    env_dict = get_env_dict(args.env, args.remote)



    bulk_copy(table_names, env_dict)

    # process prices
    dta_accss = DataAccess(os.environ['ADUB_DBServer'], PRICE_DB_NAME)
    dta_accss.executeStoredProcedure('sp_load_ice_prices_current')
    dta_accss.executeStoredProcedure('sp_load_nymex_prices_current')
    dta_accss.executeStoredProcedure('sp_load_platts_prices_current')

    generate_returns()
