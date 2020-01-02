import os
import urllib
basedir = os.path.abspath(os.path.dirname(__file__))

class Config(object):

    SERVER = os.environ['ADUB_DBServer'] or 'Lon-PC53'
    
    params = urllib.parse.quote_plus(f"DRIVER=SQL Server;SERVER={SERVER};DATABASE=Price;Trusted_Connection=yes;")

    SQLALCHEMY_DATABASE_URI = "mssql+pyodbc:///?odbc_connect=%s" % params
    SQLALCHEMY_TRACK_MODIFICATIONS = False

