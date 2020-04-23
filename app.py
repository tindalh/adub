import services.importWatcher as wtchr
from importers.energyAspectsRuns import import_energy_aspect_runs
from service_constants import *

if __name__ == '__main__':
    wtchr.watch('Rystad Production', rystadIntegrator)

    wtchr.watch(
        'Energy Aspects Runs', 
        fn=import_energy_aspect_runs, 
        file_path="{}\\EnergyAspects".format(os.environ['ADUB_Import_Path'])
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

    

    

