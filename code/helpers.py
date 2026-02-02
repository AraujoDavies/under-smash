import os
import requests
from datetime import datetime, timezone
import logging
from api_betfair import callAping
import json

def minutos_aproximados(open_date: str) -> int:
    """
        Quantos minutos faz que o evento começou

        :param open_date: data no padrão da betfair e em UTC
        :returns: Quantos minutos
        :rtype: int
    """
    # open_date = df['openDate'][0]
    try:
        sec = datetime.now(timezone.utc) - datetime.strptime(open_date, "%Y-%m-%dT%H:%M:%S.000Z").replace(tzinfo=timezone.utc)
        return int(sec.seconds / 60)
    except:
        logging.error('Falha ao converter os minutos para o jogo: %s', open_date)
        return 0


def event_time_line(event_id: str | int) -> dict:
    """
        Retorna informações como placar, tempo, etc.

        :param event_id: id do evento associado na betfair.

        :returns: informações do evento
        :rtype: dict
    """
    cookies = {
        'vid': '2821fb52-f00d-46c4-8f2b-a52f183004a5',
        '_tgpc': '1126b82a-3a7b-4aa6-abd3-afd2fd0527de',
        'OptanonAlertBoxClosed': '2025-09-28T19:10:37.989Z',
        '_gcl_au': '1.1.2096939218.1759086638',
        '_ga': 'GA1.1.401658323.1759086639',
        '_fbp': 'fb.2.1759086638651.571026515722354532',
        '_scid': 'Ft4NgHhPfdFJQkb6zlwZaxljyQ8HSBGjiUQo6Q',
        'bfsd': 'ts=1759086649847|st=reg',
        'ccawa': '56167794467873727627575413074445108557565',
        'BETEX_ESD': 'accountservices',
        'bucket': '3~18~master',
        'bfj': 'BR',
        '_ScCbts': '%5B%5D',
        'userhistory': '11025969841761356194506|3|N|021225|081125|home|N',
        'bftim': '1764701102172',
        'betexPtk': 'betexLocale%3Dpt%7EbetexRegion%3DGBR',
        'language': 'pt_BR',
        'locale': 'pt_BR',
        '_clck': '6c7qnn%5E2%5Eg1l%5E1%5E2097',
        '_tguatd': 'eyJzYyI6IihkaXJlY3QpIiwiZnRzIjoiKGRpcmVjdCkifQ==',
        '_tgidts': 'eyJzaCI6ImQ0MWQ4Y2Q5OGYwMGIyMDRlOTgwMDk5OGVjZjg0MjdlIiwiY2kiOiJkMTBkZDljNi01YjRkLTQxZjMtOWZhMC1kY2QyZGRmNjMwOTkiLCJzaSI6IjBiMDJiMTY4LWUwZjgtNDcwNy04NzAxLWZjNWRlNWI5YTUzOSJ9',
        '_tglksd': 'eyJzIjoiMGIwMmIxNjgtZTBmOC00NzA3LTg3MDEtZmM1ZGU1YjlhNTM5Iiwic3QiOjE3NjQ5NDE3MjM1MDMsInNvZCI6IihkaXJlY3QpIiwic29kdCI6MTc2NDg4Mzg4MDY4MCwic29kcyI6ImMiLCJzb2RzdCI6MTc2NDk0MTczNjU1MX0=',
        '__cf_bm': 'kXjsxlVLuv.H2d9i.2dXKz5wRn.Z8bXSv5dS15fnxBw-1764942833-1.0.1.1-UHCFgQdPoTPL8W56pMhqyP1glcW0N1RuFUrNQ_iYGKyL8bNvmyni9Tr5oTfQkKdtohKZe9eLG2qRN5BvGt8Dlbmz.AMWMzFlZy.fcdoOsq8',
        'storageSSC': 'lsSSC%3D1%3Bhidden-balance%3D1',
        'exp': 'ex',
        '_ga_DQPFWC2D61': 'GS2.1.s1764940951$o62$g1$t1764942846$j48$l0$h1960960560',
        '_scid_r': 'Jd4NgHhPfdFJQkb6zlwZaxljyQ8HSBGjiUQpQQ',
        '_rdt_uuid': '1759086638388.f552149e-fadc-42dc-b350-ded1a442ff4f',
        '_uetsid': '204998f0cee611f086fd934878b86c9c',
        '_uetvid': '39e85e204c3711f08f9283c09b975822|jikemb|1757292233967|8|1|bat.bing.com/p/insights/c/v',
        '_tgsid': 'eyJscGQiOiJ7XCJscHVcIjpcImh0dHBzOi8vd3d3LmJldGZhaXIuYmV0LmJyJTJGZXhjaGFuZ2UlMkZwbHVzJTJGXCIsXCJscHRcIjpcIkJldGZhaXIlRTIlODQlQTIlMjBFeGNoYW5nZSUyMCVDMiVCQiUyMEElMjBtYWlvciUyMGJvbHNhJTIwZGUlMjBhcG9zdGFzJTIwZG8lMjBtdW5kb1wiLFwibHByXCI6XCJcIn0iLCJwcyI6ImNmNDg0NmUxLTJmMWUtNDJkYy1iMGZlLTYzMzZjN2Q1MGRkNiIsInB2YyI6IjQiLCJzYyI6IjBiMDJiMTY4LWUwZjgtNDcwNy04NzAxLWZjNWRlNWI5YTUzOTotMSIsImVjIjoiNyIsInB2IjoiMSIsInRpbSI6IjBiMDJiMTY4LWUwZjgtNDcwNy04NzAxLWZjNWRlNWI5YTUzOToxNzY0OTQxNzI3MTEzOi0xIn0=',
        'OptanonConsent': 'isGpcEnabled=0&datestamp=Fri+Dec+05+2025+10%3A54%3A10+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=108557565&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&intType=1&geolocation=%3B&AwaitingReconsent=false',
        '_clsk': 'xhlhpx%5E1764942851323%5E10%5E1%5Ee.clarity.ms%2Fcollect',
    }

    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'no-cache',
        'origin': 'https://www.betfair.bet.br',
        'pragma': 'no-cache',
        'priority': 'u=1, i',
        'referer': 'https://www.betfair.bet.br/',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        # 'cookie': 'vid=2821fb52-f00d-46c4-8f2b-a52f183004a5; _tgpc=1126b82a-3a7b-4aa6-abd3-afd2fd0527de; OptanonAlertBoxClosed=2025-09-28T19:10:37.989Z; _gcl_au=1.1.2096939218.1759086638; _ga=GA1.1.401658323.1759086639; _fbp=fb.2.1759086638651.571026515722354532; _scid=Ft4NgHhPfdFJQkb6zlwZaxljyQ8HSBGjiUQo6Q; bfsd=ts=1759086649847|st=reg; ccawa=56167794467873727627575413074445108557565; BETEX_ESD=accountservices; bucket=3~18~master; bfj=BR; _ScCbts=%5B%5D; userhistory=11025969841761356194506|3|N|021225|081125|home|N; bftim=1764701102172; betexPtk=betexLocale%3Dpt%7EbetexRegion%3DGBR; language=pt_BR; locale=pt_BR; _clck=6c7qnn%5E2%5Eg1l%5E1%5E2097; _tguatd=eyJzYyI6IihkaXJlY3QpIiwiZnRzIjoiKGRpcmVjdCkifQ==; _tgidts=eyJzaCI6ImQ0MWQ4Y2Q5OGYwMGIyMDRlOTgwMDk5OGVjZjg0MjdlIiwiY2kiOiJkMTBkZDljNi01YjRkLTQxZjMtOWZhMC1kY2QyZGRmNjMwOTkiLCJzaSI6IjBiMDJiMTY4LWUwZjgtNDcwNy04NzAxLWZjNWRlNWI5YTUzOSJ9; _tglksd=eyJzIjoiMGIwMmIxNjgtZTBmOC00NzA3LTg3MDEtZmM1ZGU1YjlhNTM5Iiwic3QiOjE3NjQ5NDE3MjM1MDMsInNvZCI6IihkaXJlY3QpIiwic29kdCI6MTc2NDg4Mzg4MDY4MCwic29kcyI6ImMiLCJzb2RzdCI6MTc2NDk0MTczNjU1MX0=; __cf_bm=kXjsxlVLuv.H2d9i.2dXKz5wRn.Z8bXSv5dS15fnxBw-1764942833-1.0.1.1-UHCFgQdPoTPL8W56pMhqyP1glcW0N1RuFUrNQ_iYGKyL8bNvmyni9Tr5oTfQkKdtohKZe9eLG2qRN5BvGt8Dlbmz.AMWMzFlZy.fcdoOsq8; storageSSC=lsSSC%3D1%3Bhidden-balance%3D1; exp=ex; _ga_DQPFWC2D61=GS2.1.s1764940951$o62$g1$t1764942846$j48$l0$h1960960560; _scid_r=Jd4NgHhPfdFJQkb6zlwZaxljyQ8HSBGjiUQpQQ; _rdt_uuid=1759086638388.f552149e-fadc-42dc-b350-ded1a442ff4f; _uetsid=204998f0cee611f086fd934878b86c9c; _uetvid=39e85e204c3711f08f9283c09b975822|jikemb|1757292233967|8|1|bat.bing.com/p/insights/c/v; _tgsid=eyJscGQiOiJ7XCJscHVcIjpcImh0dHBzOi8vd3d3LmJldGZhaXIuYmV0LmJyJTJGZXhjaGFuZ2UlMkZwbHVzJTJGXCIsXCJscHRcIjpcIkJldGZhaXIlRTIlODQlQTIlMjBFeGNoYW5nZSUyMCVDMiVCQiUyMEElMjBtYWlvciUyMGJvbHNhJTIwZGUlMjBhcG9zdGFzJTIwZG8lMjBtdW5kb1wiLFwibHByXCI6XCJcIn0iLCJwcyI6ImNmNDg0NmUxLTJmMWUtNDJkYy1iMGZlLTYzMzZjN2Q1MGRkNiIsInB2YyI6IjQiLCJzYyI6IjBiMDJiMTY4LWUwZjgtNDcwNy04NzAxLWZjNWRlNWI5YTUzOTotMSIsImVjIjoiNyIsInB2IjoiMSIsInRpbSI6IjBiMDJiMTY4LWUwZjgtNDcwNy04NzAxLWZjNWRlNWI5YTUzOToxNzY0OTQxNzI3MTEzOi0xIn0=; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Dec+05+2025+10%3A54%3A10+GMT-0300+(Hor%C3%A1rio+Padr%C3%A3o+de+Bras%C3%ADlia)&version=202501.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=108557565&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0002%3A1%2CC0004%3A1&intType=1&geolocation=%3B&AwaitingReconsent=false; _clsk=xhlhpx%5E1764942851323%5E10%5E1%5Ee.clarity.ms%2Fcollect',
    }

    params = {
        '_ak': os.getenv('APP_KEY'),
        'alt': 'json',
        'eventId': str(event_id),
        'locale': 'pt_BR',
        'productType': 'EXCHANGE',
        'regionCode': 'UK',
    }

    response = requests.get(
        'https://ips.betfair.bet.br/inplayservice/v1/eventTimeline',
        params=params,
        cookies=cookies,
        headers=headers,
    )

    try:
        event_dict = response.json()
        # logging.info('Retornando event timeline!')
        return event_dict
    except:
        logging.critical('Falha ao buscar event timeline!')
        return {}


def get_market_book(market_id: str) -> dict:
    rpc = """{
        "jsonrpc": "2.0", 
        "method": "SportsAPING/v1.0/listMarketBook", 
        "params": { 
            "marketIds": ["market_id"],
            "marketProjection": [
                "EVENT",
                "MARKET_DESCRIPTION",
                "RUNNER_METADATA"
            ],
            "priceProjection": {      
                "priceData": ["EX_BEST_OFFERS"]
            }
        },
        "id": 1
    }""".replace('market_id', str(market_id))

    market_book = callAping(rpc)

    return market_book