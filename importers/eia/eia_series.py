from constants import EIA_API_URL, EIA_API_KEY

import requests


def get(action, id):
    url = EIA_API_URL + '/' + action + '/'
    
    response = requests.get(EIA_API_URL + '/' + action + '/', parameters(action, id))
    print(response.json()[action])
    return(response.json())


def parameters(action, id):
    return {f'{action}_id':id,"api_key":EIA_API_KEY}


if(__name__ == "__main__"):
    series_id = 'STEO.PARIPUS.M'

    get('series', series_id)
