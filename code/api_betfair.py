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
SESSION_TOKEN = []

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
  
    if SESSION_TOKEN == []:
       SESSION_TOKEN.append(session_token())
       
    headers = {'X-Application': APP_KEY, 'X-Authentication': SESSION_TOKEN[-1], 'content-type': 'application/json'}
    try:
        req = urllib.request.Request(url, jsonrpc_req.encode('utf-8'), headers)
        response = urllib.request.urlopen(req)
        jsonResponse = response.read()
        betfair_response = jsonResponse.decode('utf-8')
        if '<?xml' == betfair_response[:5]:
           return betfair_response
        resp = json.loads(betfair_response)
        if 'error' in resp.keys():
            try:
                if resp['error']['data']['APINGException']['errorCode'] == 'INVALID_SESSION_INFORMATION':
                   SESSION_TOKEN.append(session_token())
                   print('AVISO: algo deu errado... revalidando o SESSION_TOKEN')
                   return callAping(jsonrpc_req=jsonrpc_req, endpoint=url)
            except:
                pass

        return resp
    except Exception as error:
       print(error)
    except urllib.error.URLError as e:
        print (e.reason) 
        print ('Oops no service available at ' + str(url))
        return "BAD REQUEST"
    except urllib.error.HTTPError:
        print ('Oops not a valid operation from the service ' + str(url))
        return "OPERACAO INVALIDA"
    
    return "ERRO DESCONHECIDO"


def place_order(market_id: str, selection_id: str, stake: str, side: str, odd: str):
    """
        Faz a aposta.

        Args:
            stake (_str_): str no formato float com duas casas decimais, ex: 10.00
            odd (_str_): str no formato float, ex: "1.25"
            side(_str_): "BACK" ou "LAY"
    """
    place_order_req = {
        "marketId": market_id,
        "instructions": [
            {
                "selectionId": selection_id,
                "handicap":"0",
                "side": side,
                "orderType":"LIMIT",
                "limitOrder":{
                        "size": stake,
                        "price": odd,
                        "persistenceType":"LAPSE"
                    }
            }
        ],
        "customerRef": f"Daroncada{selection_id}{side}"
    }

    endPoint = 'https://api.betfair.bet.br/rest/v1.0/placeOrders/'

    place_order_response = callAping(endpoint=endPoint, jsonrpc_req=str(place_order_req).replace("'", '"'))

    return place_order_response