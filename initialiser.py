import services.brokerReceiver as brkrRcvr
import services.importWatcher as wtchr
import services.jobScheduler as jobSchdlr
from importers.arcPrices import run_daily_prices
from services.validator import validate
import datetime
import logging
import copy
import os
import sys
import schedule

from helpers.analyticsEmail import sendEmail
from cred_secrets import USERNAME, PASSWORD
from constants import EXCHANGE_SERVER, EMAIL_ADDRESS
from service_constants import *

class Initialiser(object):
    def startRefineryInfoFranchisingBrokerReceiving(self):
        refineryInfoBrokerReceiver = brkrRcvr.BrokerReceiver(
            queue='buildRefineryViews', 
            database='RefineryInfo',
            job=refineryInfoFranchiser.run
        )
        refineryInfoBrokerReceiver.run()

        return refineryInfoBrokerReceiver

    def startEIAUpdateBrokerReceiving(self):
        eiaUpdateBrokerReceiver = brkrRcvr.BrokerReceiver(
            queue='eiaUpdate', 
            database='Analytics',
            job=eiaImporter.runSeries
        )
        eiaUpdateBrokerReceiver.run()

    def startRystadWatcher(self):
        wtchr.watch('Rystad Production', rystadIntegrator)

    def startMcQuillingWatcher(self):
        wtchr.watch('McQuilling Assessments', mcQuillingIntegrator)

    def startIeaWatchers(self):
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

    def startClipperFloatingStorageWatcher(self):
        wtchr.watch('Clipper Floating Storage', clipperFloatingStorageIntegrator)

    def startEiaImportScheduler(self):
        #scheduler = schedule.every(1).minutes.do(eiaImporter.runSeries).scheduler
        scheduler = schedule.every().wednesday.at("20:00").do(eiaImporter.runSeries).scheduler
        eiaScheduler = jobSchdlr.JobScheduler('Eia Import', scheduler)
        eiaScheduler.schedule()  


    def startArcPricesScheduler(self):       

        #scheduler = schedule.every(5).minutes.do(run_daily_prices).scheduler
        scheduler = schedule.every().day.at("06:00").do(run_daily_prices).scheduler
        eiaScheduler = jobSchdlr.JobScheduler('Price Import', scheduler)
        eiaScheduler.schedule()  

    def startValidatorScheduler(self):     
        #scheduler = schedule.every(5).minutes.do(validate).scheduler
        scheduler = schedule.every().day.at("10:00").do(validate).scheduler
        validatorScheduler = jobSchdlr.JobScheduler('Validator', scheduler)
        validatorScheduler.schedule()  

    def startMcQuillingImportScheduler(self):
        #scheduler = schedule.every(10).minutes.do(mcQuilling.run).scheduler
        scheduler = schedule.every(1).day.at("14:00").do(mcQuilling.run).scheduler
        mcQuillingScheduler = jobSchdlr.JobScheduler('McQuilling Import', scheduler)
        mcQuillingScheduler.schedule()  

    def starteiSGTBrentCrudeImportScheduler(self):
        #scheduler = schedule.every(1).minutes.do(eiSGTBrentCrude.run).scheduler
        scheduler = schedule.every(1).day.at("20:00").do(eiSGTBrentCrude.run).scheduler
        jobScheduler = jobSchdlr.JobScheduler('ICE SGT Brent Crude Futures Import', scheduler)
        jobScheduler.schedule()  

    def startei1930LSGasOilImportScheduler(self):
        #scheduler = schedule.every(1).minutes.do(ei1930LSGasOil.run).scheduler
        scheduler = schedule.every(1).day.at("20:05").do(ei1930LSGasOil.run).scheduler
        jobScheduler = jobSchdlr.JobScheduler('ICE 1930 LS Gas Oil Futures Import', scheduler)
        jobScheduler.schedule() 

    def startei1630OilImportScheduler(self):
        #scheduler = schedule.every(1).minutes.do(ei1630Oil.run).scheduler
        scheduler = schedule.every(1).day.at("20:10").do(ei1630Oil.run).scheduler
        jobScheduler = jobSchdlr.JobScheduler('ICE 1630 Oil Futures Import', scheduler)
        jobScheduler.schedule() 

    def startei1630BrentCurveImportScheduler(self):
        #scheduler = schedule.every(1).minutes.do(ei1630BrentCurve.run).scheduler
        scheduler = schedule.every(1).day.at("20:15").do(ei1630BrentCurve.run).scheduler
        jobScheduler = jobSchdlr.JobScheduler('ICE 1630 Brent Curve Futures Import', scheduler)
        jobScheduler.schedule() 

