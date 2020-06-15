import sys
import os
import argparse
sys.path.append('..')
from services.exchange import account, save_email_attachments, get_emails
from helpers.log import log as log, error_email
from helpers.dataAccess import DataAccess
from constants import PRICE_DB_NAME
from importers.iceAttachments import get_max_date_imported, import_ICE_attachments

def import_email(
        email_subject, 
        file_path, 
        data_access, 
        name, 
        table_name, 
        file_parts, 
        fn_get_max_saved=None, 
        schema_name=None,
        date_column_name=None,
        fn_save=None
    ):        
    try:
        if(fn_get_max_saved is not None):
            max_saved = fn_get_max_saved(data_access, table_name, file_parts)
        else:
            max_saved = data_access.get_max_database_date(table_name, date_column_name, schema_name)

        list_emails = get_emails(
            account(),
            email_subject, 
            max_saved
        )

        save_email_attachments(list(list_emails), file_path, file_parts)

        if (fn_save is not None):
            fn_save(file_parts)

        log(__name__, '__run__', f"{name} has downloaded all emails with the subject: {email_subject}", level="Info")
    except Exception as e:
        error_email(f"{name}", import_email, f"The job has failed: {str(e)}")

if(__name__ == "__main__"):
    parser = argparse.ArgumentParser(description='Import email attachments')

    parser.add_argument(
        '--job', 
        help='The id of the job - {\n\t1: "ICE Settlement Curves"\n\
                                    \n\t2: "McQuilling Assessments"\n}', 
        required=True)

    args = parser.parse_args()

    if(args.job == '1'):
        info = [
            ('ICE 1930 LS Gas Oil Curve Futures', ['ICE 1930 LS Gas Oil Futures']),
            ('ICE 1630 SGT Futures', ['ICE 1630 SGT Brent Crude Futures','ICE 1630 SGT LS Gas Oil Futures']),
            ('ICE 1630 Oil Futures Curves', ['ICE 1630 WTI Crude Futures','ICE 1630 Heating Oil Futures','ICE 1630 (RBOB) Gasoline Futures']),
            ('ICE 1630 Brent Curve Futures', ['ICE 1630 Brent Crude Futures'])
        ]

        for i in info:
            import_email(
                email_subject=i[0], 
                file_path=os.path.join(os.environ["ADUB_Import_Path"], "ICE_Settlement"),
                data_access=DataAccess(os.environ["ADUB_DBServer"], PRICE_DB_NAME),
                name=i[0],
                table_name = 'ICE_Settlement_Curve',
                file_parts=i[1],
                fn_save = import_ICE_attachments,
                fn_get_max_saved = get_max_date_imported
            )
        
    elif(args.job == '2'):
        import_email(
                email_subject='Daily Freight Rate Assessment', 
                file_path=os.path.join(os.environ["ADUB_Import_Path"], "McQuilling\\Attachments"),
                data_access=DataAccess(os.environ["ADUB_DBServer"], PRICE_DB_NAME),
                name='McQuilling Daily Freight Rate Assessment',
                table_name = 'McQuilling',
                file_parts=['Daily Freight Rate Assessment'],
                schema_name='import',
                date_column_name='Datestamp'
            )
