import importers.eiaImporter as eia
import sys
import argparse
import helpers.dataAccess as dtAccss
import os

# python app.py -w "eia" -s "LON-PC53" -i 714755 -c "C:\Dev\Excel Files\Output\EIA" -b  "C:\Dev\Excel Files\Output\EIA"
# python app.py -w "eiaSave" -s "LON-PC53" -c "C:\Dev\Excel Files\Output\EIA" -b  "C:\Dev\Excel Files\Output\EIA"
# python app.py -w "eiaSave" -s "arcsql\mssqlserverdev"  -c "\\arcsql\RefineryInfo\RefineryUpload\EIA Data" -b  "D:\RefineryInfo\RefineryUpload\EIA Data"

parser = argparse.ArgumentParser()
parser.add_argument("-t", "--type", help="the type of download")
parser.add_argument("-s", '--server', help="the database server", default='LON-PC53')
parser.add_argument("-d", '--database', help="the database", default='Analytics')
parser.add_argument("-c", '--tempCSVFilePath', help="the path for the temp csv file", default='\\\\arcsql\\RefineryInfo\\RefineryUpload\\EIA Data')
parser.add_argument("-b", '--bulkInsertFilePath', help="the path to read for the bulk insert", default='D:\\RefineryInfo\\RefineryUpload\\EIA Data')
parser.add_argument("-r", '--rawDataFilePath', help="the path for the raw data file", default='C:\\Dev\\Excel Files')
parser.add_argument("-k", '--key', help="the api key", default='a50a785e3c8ad1b5bdd26cf522d4d473')
parser.add_argument("-u", '--url', help="the api url", default="http://api.eia.gov")
parser.add_argument("-i", '--id', help="the id to load data for", default=714755)

args = parser.parse_args()

if __name__ == '__main__':
    if(args.type.lower() == 'all'):

        eiaImporter = eia.EiaImporter(args.server, args.database, args.url, args.key, args.tempCSVFilePath, args.bulkInsertFilePath)
        eiaImporter.run(str(args.id))

    if(args.type.lower() == 'series'):

        eiaImporter = eia.EiaImporter(args.server, args.database, args.url, args.key, args.tempCSVFilePath, args.bulkInsertFilePath)
        eiaImporter.runSeries(str(args.id))
    
    