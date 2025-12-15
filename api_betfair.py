# verificando resultado da entrada utilizando a API da betfair
import requests
import urllib
import urllib.request
import urllib.error
import json
from os import getenv

from dotenv import load_dotenv
load_dotenv()

CRT_DIR = getenv('CRT_DIR')
KEY_DIR = getenv('KEY_DIR')
APP_KEY = getenv('APP_KEY')
BETFAIR_USER = getenv('BETFAIR_USER')
BETFAIR_PASSWORD = getenv('BETFAIR_PASSWORD')


def session_token():
  payload = f"username={BETFAIR_USER}&password={BETFAIR_PASSWORD}"
  headers = {'X-Application': APP_KEY, 'Content-Type': 'application/x-www-form-urlencoded'}
  resp = requests.post('https://identitysso-cert.betfair.bet.br/api/certlogin', data=payload, cert=(CRT_DIR, KEY_DIR), headers=headers)
  
  if resp.status_code == 200:
    resp_json = resp.json()
    print (resp_json['loginStatus'])
    return resp_json['sessionToken']
  else:
    print ("Request failed.")


def callAping(jsonrpc_req: str, endpoint = None) -> dict:
    """
      Bate na API da betfair para coletar dados

      jsonrpc_req (_str_) -> filtro (SEE MORE: https://docs.developer.betfair.com/display/1smk3cen4v3lu3yomq5qye0ni/Getting+Started#GettingStarted-ExampleRequests)

      return game_data
    """
    if endpoint is None:
        url = "https://api.betfair.bet.br/exchange/betting/json-rpc/v1"
    else:
        url = endpoint
  
    headers = {'X-Application': APP_KEY, 'X-Authentication': session_token(), 'content-type': 'application/json'}
    try:
        req = urllib.request.Request(url, jsonrpc_req.encode('utf-8'), headers)
        response = urllib.request.urlopen(req)
        jsonResponse = response.read()
        return jsonResponse.decode('utf-8')
    except urllib.error.URLError as e:
        print (e.reason) 
        print ('Oops no service available at ' + str(url))
    except urllib.error.HTTPError:
        print ('Oops not a valid operation from the service ' + str(url))


def api_betfair(id_do_matchodds):
    list_prices = '{"jsonrpc": "2.0", "method": "SportsAPING/v1.0/listMarketBook", "params": { "marketIds": ["'+id_do_matchodds+'"], "priceProjection": {"priceData": ["EX_BEST_OFFERS", "EX_TRADED"],"virtualise": "true"}},"id": 1}'
    call = callAping(list_prices)
    l = json.loads(call)
    return l