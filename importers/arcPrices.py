import subprocess
import sys
import os
sys.path.append('..')
from helpers.utils import get_project_root

# Constant
ROOT_DIR = get_project_root()
SOURCE_DB_SCHEMA = "ARC_DOCDROP.dbo"
TARGET_DB_SCHEMA = "Price.import"

# Change with environment
source_server_name = "ARCBO8\\DEV"
target_server_name = "arcsql.arcpet.co.uk"

# Changing
current_table_names = [
    {
        "Source": "TEMP_LANDED_MFL_L1_ICE_1",
        "Target": "ICE_L1_1_Current"
    },
    {
        "Source": "view_TEMP_LANDED_MFL_L1_ICE_5",
        "Target": "ICE_L1_5_Current"
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
    }
]

# Changing
history_table_names = [
    {
        "Source": "WRK_VAULT_MFL_L1_ICE_1",
        "Target": "ICE_L1_1_History"
    },
    {
        "Source": "view_WRK_VAULT_MFL_L1_ICE_5",
        "Target": "ICE_L1_5_History"
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
    }
]

def copy_current():
    bulk_copy(current_table_names)

def copy_history():
    bulk_copy(history_table_names)

def bulk_copy(table_names):

    for table in table_names:
        source_table_name = table["Source"]
        target_table_name = table["Target"]

        try:
            subprocess.run(  # bcp to a file
                [
                    # the batch file
                    os.path.join(ROOT_DIR, "batch", "bcp.out.file.bat"),
                    f"{SOURCE_DB_SCHEMA}.{source_table_name}",
                    f"c:\\temp\\data.{source_table_name}.txt",
                    os.path.join(ROOT_DIR, "format",
                                f"format.{source_table_name}.fmt"),
                    source_server_name
                ], check=True
            )

            subprocess.run(  # truncate target table
                [
                    # the batch file
                    os.path.join(ROOT_DIR, "batch", "sqlcmd.truncate.bat"),
                    target_server_name,
                    f"{TARGET_DB_SCHEMA}.{target_table_name}"
                ], check=True
            )

            subprocess.run(  # bcp from a file
                [
                    # the batch file
                    os.path.join(ROOT_DIR, "batch", "bcp.in.file.bat"),
                    f"{TARGET_DB_SCHEMA}.{target_table_name}",
                    f"c:\\temp\\data.{source_table_name}.txt",
                    os.path.join(ROOT_DIR, "format",
                                f"format.{source_table_name}.fmt"),
                    target_server_name,
                ], check=True
            )
        except Exception as cpe:
            print(str(cpe))


if(__name__ == "__main__"):
    if(len(sys.argv) > 1):
        if(sys.argv[1] == 'history'):
            copy_history()
        if(sys.argv[1] == 'current'):
            copy_current()
    else:
        bulk_copy(current_table_names)
