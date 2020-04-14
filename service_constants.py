import franchisers.refineryInfoFranchiser as refineryInfoFrnchsr
import integrators.csvIntegrator as csvIntgrtr
from importers.mcQuilling import McQuilling
from cleaners.ieaTxtCleaner import clean as cleanIeaTxt
from cleaners.rystadCleaner import clean as rystadCleaner
from cleaners.clipperFloatingStorageCleaner import clean as clipperFloatingStorageCleaner
import importers.eiaImporter as eiaImprtr
from importers.emailImporter import EmailImporter
from importers.iceAttachments import get_max_date_imported, import_ICE_attachments
import os

ieaSupplyIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Supply',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='supply',
    file_columns=['Country','Product','Period','Quantity'],
    table_columns=['Country','Product','Period','PeriodType','Quantity','Asof'],
    file_name='SUPPLY.TXT',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter='\s+',
    truncate=False,
    keys=['Asof'],
)

ieaStockdatIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Stock Data',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='stockdat',
    file_columns=['Stock','Country','Product','Period','Quantity'],
    table_columns=['Stock','Country','Product','Period','PeriodType','Quantity','Asof'],
    file_name='stockdat.TXT',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter='\s+',
    truncate=False,
    keys=['Asof'],
)

ieaSummaryIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Summary',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='Summary',
    file_columns=['Geography','Final','Period','Quantity'],
    table_columns=['Geography','Final','Period','PeriodType','Quantity','Asof'],
    file_name='Summary.TXT',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter='\s+',
    truncate=False,
    keys=['Asof'],
)

ieaNOECDDEIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA NOECDDE',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='NOECDDE',
    file_columns=['Country','Period','Quantity'],
    table_columns=['Country','Period','PeriodType','Quantity','Asof'],
    file_name='NOECDDE.TXT',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter='\s+',
    truncate=False,
    keys=['Asof'],
)

ieaOECDDEIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA OECDDE',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='OECDDE',
    file_columns=['Country','PRODUCT','Period','Quantity'],
    table_columns=['Country','Product','Period','PeriodType','Quantity','Asof'],
    file_name='OECDDE.TXT',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter='\s+',
    truncate=False,
    keys=['Asof'],
)

ieaCrudeDataIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Crude Data',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='CRUDEDAT',
    file_columns=['COUNTRY','PRODUCT','BALANCE','Period','Quantity'],
    table_columns=['Country','Product','Balance','Period','PeriodType','Quantity','Asof'],
    file_name='CRUDEDAT.TXT',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter='\s+',
    truncate=False,
    keys=['Asof'],
)

ieaExportDataIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Export Data',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='ExporDAT',
    file_columns=['COUNTRY','PRODUCT','Export Country','Period','Quantity'],
    table_columns=['Country','Product','Export Country','Period','PeriodType','Quantity','Asof'],
    file_name='EXPORDAT.TXT',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter='\s+',
    truncate=False,
    keys=['Asof'],
)

ieaImportDataIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Import Data',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='ImporDAT',
    file_columns=['COUNTRY','PRODUCT','Import Country','Period','Quantity'],
    table_columns=['Country','Product','Import Country','Period','PeriodType','Quantity','Asof'],
    file_name='ImPORDAT.TXT',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter='\s+',
    truncate=False,
    keys=['Asof'],
)

ieaProdDataIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Prod Data',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='ProdDAT',
    file_columns=['PRODUCT','COUNTRY','BALANCE','Period','Quantity'],
    table_columns=['Product','Country','Balance','Period','PeriodType','Quantity','Asof'],
    file_name='PRODDAT.TXT',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter='\s+',
    truncate=False,
    keys=['Asof'],
)


ieaFieldIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Fields',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='field_by_field',
    file_columns=['FIELD', 'COUNTRY','PRODUCT','ENVIRONMENT','TIME','FREQUENCY','TIMESTAMP','VALUE'],
    table_columns=['Field','Country','Product','Environment','Period','PeriodType','Value','Asof'],
    file_name='field_by_field.csv',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter=',',
    truncate=False,
    keys=['Asof'],
)

ieaCountryDetailsIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Country Details',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='country_details',
    table_columns=['COUNTRY_CODE','COUNTRY_NAME','ISO_ALPHA_2','ISO_ALPHA_3'],
    file_name='country_details.csv',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    delimiter=',',
    truncate=True
)

ieaFieldDetailsIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Field Details',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='field_details',
    table_columns=['FIELD_CODE','FIELD_NAME','COUNTRY','GROUP_CODE','GROUP_NAME','PRODUCT','ENVIRONMENT'],
    file_name='field_details.csv',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    delimiter=',',
    truncate=True
)

rystadIntegrator = csvIntgrtr.CsvIntegrator(
    name='Rystad Production',
    server=os.environ['ADUB_DBServer'],
    database='Analytics',
    table_name='yview_RystadProduction',
    table_columns=['Country','Category','Grade','Period','Production','SulphurGroup','SulphurDetail','CleanSulphurDetail'],
    file_path="{}\\RystadProduction".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\RystadProduction\\".format(os.environ['ADUB_Import_Output_UNC']), 
    truncate=False,
    keys=['Period'],
    clean = rystadCleaner,
    delimiter=','
)

clipperFloatingStorageIntegrator = csvIntgrtr.CsvIntegrator(
    name='Clipper Floating Storage',
    server=os.environ['ADUB_DBServer'],
    database='STG_Targo',
    table_name='yview_ClipperFloatingStorage',
    file_path="{}\\ClipperFloatingStorage".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\ClipperFloatingStorage\\".format(os.environ['ADUB_Import_Output_UNC']), 
    truncate=False,
    keys=['date_asof'],
    clean = clipperFloatingStorageCleaner,
    clean_arg='2015-01-01',
    delimiter=','
)

mcQuillingIntegrator = csvIntgrtr.CsvIntegrator(
    name='McQuilling Assessments',
    server=os.environ['ADUB_DBServer'],
    database='Price',
    table_name='import.McQuilling',
    table_columns=['DateStamp','Class','Voyage','Tons','WS','TCE','Demurrage','Comments', 'VoyageType', 'IsDirty'],
    file_path="{}\\McQuilling".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\McQuilling\\".format(os.environ['ADUB_Import_Output_UNC']), 
    truncate=False,
    delimiter='|',
    keys=['DateStamp'],
    post_op_procedure='sp_McQuilling_Load',
)

eiaImporter = eiaImprtr.EiaImporter(
    server=os.environ['ADUB_DBServer'], 
    database='Analytics',
    url="http://api.eia.gov",
    api_key='a50a785e3c8ad1b5bdd26cf522d4d473',
    file_path="{}\\EIA".format(os.environ['ADUB_Import_Output_UNC']),
    bulkinsert_path="{}\\EIA".format(os.environ['ADUB_Import_Output'])
)

        
mcQuilling = McQuilling(
    'Daily Freight Rate Assessment',
    "{}\\McQuilling".format(os.environ['ADUB_Import_Path']),
    database_server=os.environ['ADUB_DBServer'],
    database='Price'
)

eiSGTBrentCrude = EmailImporter(
    'ICE 1630 SGT Futures',
    "{}\\ICE_Settlement".format(os.environ['ADUB_Import_Path']),
    database_server=os.environ['ADUB_DBServer'],
    database='Price',
    name='ICE 1630 SGT Futures',
    table_name = 'ICE_Settlement_Curve',
    file_parts = ['ICE 1630 SGT Brent Crude Futures','ICE 1630 SGT LS Gas Oil Futures'],
    fn_save = import_ICE_attachments,
    fn_get_max_saved = get_max_date_imported
)

ei1930LSGasOil = EmailImporter(
    'ICE 1930 LS Gas Oil Curve Futures',
    "{}\\ICE_Settlement".format(os.environ['ADUB_Import_Path']),
    database_server=os.environ['ADUB_DBServer'],
    database='Price',
    name='ICE 1930 LS Gas Oil Curve Futures',
    table_name = 'ICE_Settlement_Curve',
    file_parts=['ICE 1930 LS Gas Oil Futures'],
    fn_save = import_ICE_attachments,
    fn_get_max_saved = get_max_date_imported
)

ei1630Oil = EmailImporter(
    'ICE 1630 Oil Futures Curves',
    "{}\\ICE_Settlement".format(os.environ['ADUB_Import_Path']),
    database_server=os.environ['ADUB_DBServer'],
    database='Price',
    name='ICE 1630 Oil Futures Curves',
    table_name = 'ICE_Settlement_Curve',
    file_parts=['ICE 1630 WTI Crude Futures','ICE 1630 Heating Oil Futures','ICE 1630 (RBOB) Gasoline Futures'],
    fn_save = import_ICE_attachments,
    fn_get_max_saved = get_max_date_imported
)

ei1630BrentCurve = EmailImporter(
    'ICE 1630 Brent Curve Futures',
    "{}\\ICE_Settlement".format(os.environ['ADUB_Import_Path']),
    database_server=os.environ['ADUB_DBServer'],
    database='Price',
    name='ICE 1630 Brent Curve Futures',
    table_name = 'ICE_Settlement_Curve',
    file_parts=['ICE 1630 Brent Crude Futures'],
    fn_save = import_ICE_attachments,
    fn_get_max_saved = get_max_date_imported
)

refineryInfoFranchiser = refineryInfoFrnchsr.RefineryInfoFranchiser(
    os.environ['ADUB_DBServer'], 'RefineryInfo'
)



ieaSplitdatIntegrator = csvIntgrtr.CsvIntegrator(
    name='IEA Split Data',
    server=os.environ['ADUB_DBServer'],
    database='IEAData',
    table_name='splitdat',
    file_columns=['Product','Country','Balance','Period','Quantity'],
    table_columns=['Product','Country','Balance','Period','PeriodType','Quantity','Asof'],
    file_name='splitdat.TXT',
    file_path="{}\\IEA".format(os.environ['ADUB_Import_Path']), 
    output_file_path="{}\\IEA\\".format(os.environ['ADUB_Import_Output_UNC']), 
    clean = cleanIeaTxt,
    delimiter='\s+',
    truncate=False,
    keys=['Asof'],
)


if(__name__ == "__main__"):
    ei1630BrentCurve.run()
    ei1630Oil.run()
    ei1930LSGasOil.run()
    eiSGTBrentCrude.run()