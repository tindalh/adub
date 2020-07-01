import services.importWatcher as wtchr
from importers.energyAspectsRuns import import_energy_aspect_runs
from importers.fge_runs import import_fge_runs
from importers.refinitiv_afra_floating_storage import import_refinitiv_afra_storage
from importers.refinitiv_suezmax_floating_storage import import_refinitiv_suezmax_storage
from importers.refinitiv_vlcc_floating_storage import import_refinitiv_vlcc_storage
from service_constants import *

if __name__ == '__main__':
    wtchr.watch('Rystad Production', rystadIntegrator)

    wtchr.watch(
        'Energy Aspects Runs', 
        fn=import_energy_aspect_runs, 
        file_path="{}\\EnergyAspects".format(os.environ['ADUB_Import_Path'])
    )

    wtchr.watch(
        'FGE Runs', 
        fn=import_fge_runs, 
        file_path="{}\\FGE".format(os.environ['ADUB_Import_Path'])
    )

    wtchr.watch(
        'Refinitiv Aframax', 
        fn=import_refinitiv_afra_storage, 
        file_path="{}\\Refinitiv\\Aframax".format(os.environ['ADUB_Import_Path'])
    )

    wtchr.watch(
        'Refinitiv Suezmax', 
        fn=import_refinitiv_suezmax_storage, 
        file_path="{}\\Refinitiv\\Suezmax".format(os.environ['ADUB_Import_Path'])
    )

    wtchr.watch(
        'Refinitiv VLCC', 
        fn=import_refinitiv_vlcc_storage, 
        file_path="{}\\Refinitiv\\VLCC".format(os.environ['ADUB_Import_Path'])
    )

    wtchr.watch('McQuilling Assessments', mcQuillingIntegrator)

    wtchr.watch('IEA Supply', ieaSupplyIntegrator)
    wtchr.watch('IEA Stock Data', ieaStockdatIntegrator)
    wtchr.watch('IEA Split Data', ieaSplitdatIntegrator)
    wtchr.watch('IEA Summary', ieaSummaryIntegrator)
    wtchr.watch('IEA NOECDDE', ieaNOECDDEIntegrator)
    wtchr.watch('IEA OECDDE', ieaOECDDEIntegrator)
    wtchr.watch('IEA Crude Data', ieaCrudeDataIntegrator)
    wtchr.watch('IEA Export Data', ieaExportDataIntegrator)
    wtchr.watch('IEA Import Data', ieaImportDataIntegrator)
    wtchr.watch('IEA Prod Data', ieaProdDataIntegrator)
    wtchr.watch('IEA Fields', ieaFieldIntegrator)
    wtchr.watch('IEA Country Details', ieaCountryDetailsIntegrator)
    wtchr.watch('IEA Field Details', ieaFieldDetailsIntegrator)

    wtchr.watch('Clipper Floating Storage', clipperFloatingStorageIntegrator)

    

    

