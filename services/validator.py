import sys
sys.path.append('..')
from helpers.dataAccess import DataAccess
from helpers.log import error_email
from constants import TARGO_DB_NAME, INTERNAL_DOMAIN
import os

def validate():
    targo_duplicate_locations()

def targo_duplicate_locations():
    """
        Checks for duplicate locations after the Clipper job has run
    """
    data_access = DataAccess(os.environ['ADUB_DBServer'], TARGO_DB_NAME)

    if(len(data_access.load('view_duplicate_locations')) > 0):
        error_email(__name__, 'targo_duplicate_locations', "There are duplicate locations in Targo")


if(__name__ == '__main__'):
    validate()
    