import schedule
import time

from api_betfair import callAping, place_order
import logging
from datetime import datetime, timedelta, timezone
import pandas as pd

from helper_db import engine, TblUnderSmash, session
from sqlalchemy import select
from helpers import *
from helper_telegram import enviar_no_telegram
from typing import List, Dict


from dotenv import load_dotenv
load_dotenv()

STAKE = os.getenv("STAKE")
JA_FEZ_CASHOUT = []
ignorar_events = [] # add o id dos eventos que sairam do padrão

log_file = datetime.now().strftime("./logs/under-smash-%d-%m-%Y.log")
print('LOG FILE: ', log_file)
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    encoding='utf-8',
    format='%(asctime)s - %(levelname)s: %(message)s'
)


def analisa_jogos_em_andamento():
    # listando jogos em andamento
    format_strdt = '%Y-%m-%dT%H:%M:%SZ'
    agr_utc = datetime.now(timezone.utc)
    maisxh_utc = agr_utc + timedelta(hours=2)
    dia_from = agr_utc.strftime(format_strdt)
    dia_to = maisxh_utc.strftime(format_strdt)

    rpc = """
    {
        "jsonrpc": "2.0",
        "method": "SportsAPING/v1.0/listEvents",
        "params": {
            "filter": {
                "eventTypeIds": [
                    "1"
                ],
                "inPlayOnly": "true"
            }
        },
    "id": 1}
    """.replace('dia_from', dia_from).replace('dia_to', dia_to)

    list_events = callAping(rpc)

    jogos_do_dia = []
    for jogo in list_events['result']:
        jogos_do_dia.append(jogo['event'])

    df_events = pd.DataFrame(jogos_do_dia)
    logging.info("Jogos em andamento: %s", len(df_events))

    if df_events.empty:
        return ''
    
    df_events['minuto_aproximado'] = df_events['openDate'].map(lambda x: minutos_aproximados(x)) # Quantos minutos aprox o jogo tem

    df_events = df_events.sort_values('openDate')
    df_events.rename(columns={"id": "event_id"}, inplace=True)

    df_events = df_events[["event_id", "name", "minuto_aproximado"]]
    # df_events

    # Coletar status, placar, total de gols e tempo real de jogo
    df = df_events[df_events["minuto_aproximado"] > 45].copy()

    for index in df.index:
        event_id = df.loc[index]['event_id']
        try:
            if event_id in ignorar_events:
                df.loc[index, "status"] = "LISTA DE IGNORADOS"
                df.loc[index, "inPlayMatchStatus"] = "-"
                df.loc[index, "mercado"] = '-'
                continue

            match_info = event_time_line(event_id)
            home_score = match_info["score"]["home"]["score"]
            away_score = match_info["score"]["away"]["score"]
            mercado = str(float(home_score) + float(away_score) + 0.5)

            df.loc[index, "placar"] = f"{home_score} - {away_score}"
            df.loc[index, "mercado"] = f"Over/Under {mercado} Goals"
            df.loc[index, "tempo"] = match_info["timeElapsed"]
            df.loc[index, "inPlayMatchStatus"] = match_info["inPlayMatchStatus"]
            df.loc[index, "status"] = match_info["status"]
        except Exception as error:
            df.loc[index, "status"] = "ERROR"
            df.loc[index, "inPlayMatchStatus"] = "-"
            df.loc[index, "mercado"] = '-'

    # df

    if df.empty:
        return ''
    
    # ignorar analise dos eventos que não estejam no intervalo
    df_to_ignore = df[df["inPlayMatchStatus"] != "FirstHalfEnd"].copy()
    for id in list(df_to_ignore['event_id']):
        if id not in ignorar_events: ignorar_events.append(id)

    # coletar marketId (mercado do limite)
    df = df[df["inPlayMatchStatus"] == "FirstHalfEnd"].copy()
    # df = df[~df["mercado"].isna()].copy() # HACK
    # df
    logging.info("Jogos que estão no HT: %s", len(df))

    for index in df.index:
        event_id = df.loc[index]['event_id']
        mercado = df.loc[index]['mercado']

        rpc = """
        {
        "jsonrpc": "2.0",
        "method": "SportsAPING/v1.0/listMarketCatalogue",
        "params": {
            "filter": {
                "eventIds": ["event_id"]
            },
            "maxResults": "200"
        },
        "id": 1
        }
        """.replace('event_id', str(event_id))

        market_catalogue = callAping(rpc)

        if market_catalogue['result'] == []: continue

        df_catalogue = pd.DataFrame(market_catalogue['result'])

        market_id = df_catalogue[df_catalogue["marketName"] == mercado]['marketId']
        if market_id.empty: continue

        market_id = market_id[market_id.first_valid_index()]

        df.loc[index, "market_id"] = market_id

    # df

    # coletar ODDs e acrescentar no banco caso seja menor que @1.18
    for index in df.index:
        market_id = df.loc[index, "market_id"]
        
        market_book = get_market_book(str(market_id))

        try:
            runners = market_book["result"][0]["runners"][1]["ex"]
            df.loc[index, "lay_under"] = runners["availableToLay"][0]["price"]
            df.loc[index, "odd_max_saida"] = runners["availableToBack"][0]["price"]
            df.loc[index, "total_correspondido"] = market_book['result'][0]['totalMatched']
            df.loc[index, "selection_id"] = str(market_book['result'][0]['runners'][1]['selectionId'])
        except:
            ...

        df.loc[index, "dt_insert"] = datetime.now()
        df.loc[index, "dt_last_update_odd"] = datetime.now()
        df.loc[index, "dt_market_closed"] = datetime.now()

    if 'minuto_aproximado' in df.columns:
        df.pop('minuto_aproximado')
    if 'lay_under' in df.columns:
        df = df[df['lay_under'] <= 1.18].copy()
    # df

    # adicionar no DB
    for index in df.index:
        try:
            df_db = df[df.index == index].copy()
            insert = df_db.to_sql(name="under_smash", con=engine, if_exists="append", index=False)
            if insert == 1:
                logging.info('Jogo adicionado no banco: %s', df_db.loc[df_db.first_valid_index(), 'name'])
                # enviar sinal
                if df_db.loc[df_db.first_valid_index(), 'total_correspondido'] > float(os.getenv("LIQUIDEZ_MINIMA")):
                    msg = """⚽️ <b>Lay Over</b> 😮‍💨

[{event}]({link})

Stake 10%

» Entre a @{odd} | Feche a posição em Back @{odd_fecho} ⚠️
"""
                    link = 'https://www.betfair.bet.br/exchange/plus/football/market/' + df_db.loc[df_db.first_valid_index(), 'market_id']
                    odd = str(round(df_db.loc[df_db.first_valid_index(), 'lay_under'], 2))
                    odd_fecho = str(round(df_db.loc[df_db.first_valid_index(), 'lay_under'] + 0.1, 2))
                    event = df_db.loc[df_db.first_valid_index(), 'name'].replace(' v ', f" {df_db.loc[df_db.first_valid_index(), 'placar']} ")
                    msg = msg.replace('{odd}', odd).replace('{odd_fecho}', odd_fecho).replace('{event}', event).replace('{link}', link)
                    enviar_no_telegram(chat_id=os.getenv('TELEGRAM_CHAT_ID'), msg=msg)
        except Exception as error:
            logging.error("INSERT: %s", str(error))
            pass

    # ignorar eventos que estão no banco
    for id in list(df['event_id']):
        if id not in ignorar_events: ignorar_events.append(id)


# Atualizar eventos que estão no banco
def atualizar_eventos_em_andamento():
    stmt = select(TblUnderSmash).where(TblUnderSmash.status == 'IN_PLAY')
    matchs_inplay = session.execute(stmt).scalars().all()
    if len(matchs_inplay) > 0:
        logging.info('Eventos em andamento (INPLAY): %s', len(matchs_inplay))
    # matchs_inplay

    for row in matchs_inplay:
        market_book = get_market_book(row.market_id)
        
        status_over = market_book["result"][0]["runners"][1]["status"] # ACTIVE, LOSER ou WINNER
        status_market = market_book["result"][0]["status"] # CLOSED, OPEN, SUSPENDED

        if status_market == 'OPEN':
            if status_over == 'ACTIVE':
                odd_now = market_book["result"][0]["runners"][1]["ex"]["availableToBack"][0]["price"]
                if odd_now > row.odd_max_saida:
                    row.odd_max_saida = odd_now
                    row.dt_last_update_odd = datetime.now()
            if status_over == "LOSER": # indica que a odd max de saida foi 1000
                row.odd_max_saida = 1000
                row.dt_last_update_odd = datetime.now()
                row.dt_market_closed = datetime.now()
                row.status = "FINISH"
            if status_over == "WINNER": # significa que saiu um gol ai nao posso atualizar mais a odd.
                row.dt_market_closed = datetime.now()
                row.status = "FINISH"

        if status_market == "CLOSED":
            row.dt_market_closed = datetime.now()
            row.status = "FINISH"
        
    session.commit()


def monitorar_entradas():
    """
        faz a entrada e monitora se foi correspondido e quando deve fechar a posição. Entra em Lay ao over e fecha 10 ticks acima.
    """
    stmt = select(TblUnderSmash).where(TblUnderSmash.status=="IN_PLAY", TblUnderSmash.total_correspondido>=float(os.getenv("LIQUIDEZ_MINIMA")))
    matchs_in_play = session.execute(stmt).scalars().all()
    
    # doing pending bets
    for m in matchs_in_play:
        if m in JA_FEZ_CASHOUT: continue
        # check if already have some bet matched.
        payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listCurrentOrders",
            "params": {
                "betStatus": "EXECUTABLE",
                "marketIds": [m.market_id]
            },
            "id": 1
        }
        payload = str(payload).replace("'", '"')
        o = callAping(jsonrpc_req=payload)

        apostas = []
        for a in o['result']['currentOrders']:
            if str(a['marketId']) == m.market_id and str(a['selectionId']) == m.selection_id: # do banco ja vem como string
                apostas.append({
                    # 'market_id': a['marketId'],
                    # 'selection_id': a['selectionId'],
                    'tipo': a['side'],
                    'stake': a['sizeMatched'],
                    'odd': a['averagePriceMatched'],
                })

        market_book = get_market_book(str(m.market_id))
        market_status = market_book['result'][0]['status']
        if market_status != 'OPEN': 
            if market_status not in ['SUSPENDED']:
                print(m.name, market_book)
            continue

        # canceling pending bets
        payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/cancelOrders",
            "params": {
                "marketId": m.market_id
            },
            "id": 1
        }
        payload = str(payload).replace("'", '"')
        o = callAping(jsonrpc_req=payload)
        try:
            if o['result']['instructionReports']:
                logging.info("Did cancel bet: %s", o)
        except:
            logging.warning("Failed to cancel bet: %s", o)


        # if have bet, check status and if it have to leave 
        if bool(apostas):
            odd_fecho = round(m.lay_under + 0.1, 2)
            cash_info = calcular_cashout(apostas=apostas, odd_atual_back=odd_fecho, odd_atual_lay=odd_fecho)
            if float(cash_info['stake_hedge']) < float(STAKE) * 0.2: # only if hedge < 20% from stake
                if len(apostas) > 1: # in case of two or more bets...
                    logging.info('Ignoring this event: %s\nResult: %s', m.name, cash_info)
                    JA_FEZ_CASHOUT.append(m)
                continue # stake hedge == 0 nothing to do in this market

            try:
                runners = market_book["result"][0]["runners"][1]["ex"]
                odd_lay = runners["availableToLay"][0]["price"] 
                odd_back = runners["availableToBack"][0]["price"]
                peso_dinheiro_back = runners["availableToLay"][0]["size"] # pela ladder vc vê q é o inverso mesmo
                peso_dinheiro_lay = runners["availableToBack"][0]["size"] # pela ladder vc vê q é o inverso mesmo
                gap = odd_lay - odd_back
            except:
                logging.error("%s runners error: %s", m, market_book)
                continue

            if odd_back >= odd_fecho:
                cash_info = calcular_cashout(apostas=apostas, odd_atual_back=odd_back, odd_atual_lay=odd_lay)
                logging.info("Need to do cashout: %s", cash_info)
                order_response = str(place_order(
                    market_id=m.market_id, selection_id=m.selection_id, stake=cash_info['stake_hedge'], side="BACK", odd=str(odd_fecho)
                ))
                logging.info("did cashout, status: %s", order_response)


        # if have no bet, do lay over
        if bool(apostas) == False:           
            market_id = m.market_id
            selection_id = m.selection_id
            odd = round(m.lay_under, 2)

            order_response = str(place_order(
                market_id=market_id, selection_id=selection_id, stake=STAKE, side="LAY", odd=str(odd)
            ))
            logging.info("did bet, status: %s", order_response)
            if '<status>SUCCESS</status>' in order_response:
                m.betfair_response_entrada = order_response
                m.dt_entrada = datetime.now()
                m.stake = STAKE
                # telegram
                MSG = f'LAY OVER: **{m.name}** ⚽️⏰ R$ {STAKE}'
                telegram_id = enviar_no_telegram(chat_id=os.getenv('TELEGRAM_CHAT_ID_DEBUG'), msg=MSG)

    session.commit()


def calcular_cashout(apostas: List[Dict], odd_atual_back: float, odd_atual_lay: float):
    """
    Faz o calculo do cashout
    Parameters:
        apostas: lista de dicionários no formato:
            {
                "tipo": "back" ou "lay",
                "stake": float,
                "odd": float
            }
        odd_atual_back: odd em que você vai fechar se back
        odd_atual_back: odd em que você vai fechar se lay
    Returns:
        dict: infos do cash
    """

    possivel_green = 0.0  # resultado se ganhar
    possivel_red = 0.0  # resultado se perder

    for a in apostas:
        stake = a["stake"]
        odd = a["odd"]
        tipo = a["tipo"].lower()

        if tipo == "back":
            possivel_green += stake * (odd - 1)
            possivel_red -= stake
        elif tipo == "lay":
            possivel_green -= stake * (odd - 1)
            possivel_red += stake
        else:
            raise ValueError("Tipo de aposta inválido: use 'back' ou 'lay'")

    if possivel_red < possivel_green:
        mercado_saida = "lay"
    else:
        mercado_saida = 'back'

    if mercado_saida == 'lay':
        stake_hedge = (possivel_green - possivel_red) / odd_atual_lay
    else: # BACK
        stake_hedge = (possivel_red - possivel_green) / odd_atual_back

    resultado_final = possivel_green - stake_hedge * (odd_atual_lay - 1) if mercado_saida == "lay" else possivel_green + stake_hedge * (odd_atual_back - 1)

    return {
        "mercado_saida_hedge": mercado_saida.upper(),
        "stake_hedge": str(round(stake_hedge, 2)),
        "resultado_apos_hedge": round(resultado_final, 2),
        "resultado_se_01": round(possivel_green, 2), # se mercado bater 1.01 (vencedor)
        "resultado_se_1000": round(possivel_red, 2), # se mercado for a mil (perdedor)
    }


def saida_cashout(match_db: TblUnderSmash, odd_back: float, odd_lay: float) -> tuple:
    """Faz saída em cashout
    
    Args:

        market_id (_str_): id do mercado para cash
        odd_back (_float_): odd back atual
        odd_lay (_float_): odd lay atual
    
    Returns:

        status (_tuple_): contendo resultado da operacao + mensagem de erro ou placebet response

    Example:

        >>> saida_cashout()
        (True, "<xml>...")

    """
    payload = """{
        "jsonrpc": "2.0",
        "method": "SportsAPING/v1.0/listCurrentOrders",
        "params": {
            "betStatus": "EXECUTABLE"
        },
        "id": 1
    }"""
    o = callAping(jsonrpc_req=payload)
    apostas = []
    for a in o['result']['currentOrders']:
        if str(a['marketId']) == match_db.market_id and str(a['selectionId']) == match_db.selection_id: # do banco ja vem como string
            apostas.append({
                # 'market_id': a['marketId'],
                # 'selection_id': a['selectionId'],
                'tipo': a['side'],
                'stake': a['sizeMatched'],
                'odd': a['averagePriceMatched'],
            })

    # df = pd.DataFrame(apostas)

    info = calcular_cashout(apostas, odd_back, odd_lay)

    if info['resultado_se_01'] == info['resultado_se_1000'] or float(info['stake_hedge']) < 0.10:
        return (True, "HEDGE SUCCESS!")
    
    logging.info('cashout: %s', info)

    odd_saida = odd_back if info['mercado_saida_hedge'] == 'back' else odd_lay
    place_order_resp = place_order(market_id=match_db.market_id, selection_id=match_db.selection_id, stake=info['stake_hedge'], side=info['mercado_saida_hedge'], odd=odd_saida)

    if 'MARKET_SUSPENDED' in place_order_resp:
        return (False, "Mercado suspenso!")
    if 'INVALID_ODDS' in place_order_resp:
        return (False, f"ODD Inválida! @{odd_saida}")
    enviar_no_telegram(chat_id=os.getenv('TELEGRAM_CHAT_ID_DEBUG'), msg=f"Fechou posição de {match_db.name} em {info['mercado_saida_hedge']} @{odd_saida} com stake {info['stake_hedge']}. Resultado após hedge: R$ {info['resultado_apos_hedge']}")
    return (True, place_order_resp)


def atualizar_pl():
    filter_data = datetime.now() - timedelta(days=2)
    stmt = select(TblUnderSmash).where(TblUnderSmash.status == "FINISH", TblUnderSmash.profit == 0, TblUnderSmash.dt_entrada > filter_data)

    att_pl = session.execute(stmt).scalars().all()

    for match_db in att_pl:
        payload = {
            "jsonrpc": "2.0",
            "method": "SportsAPING/v1.0/listClearedOrders",
            "params": {
                "marketIds": [match_db.market_id],
                "betStatus": "SETTLED",
                "recordCount": 200
            },
            "id": 1
        }

        payload = str(payload).replace("'", '"')

        try:
            cleared_orders = callAping(jsonrpc_req=payload)
        except:
            continue

        try:
            df = pd.DataFrame(cleared_orders['result']['clearedOrders'])
            if df.empty: 
                continue
        except:
            logging.error('market_id cleared_orders: %s', match_db.market_id)
            continue

        pl_float = float(df['profit'].sum())
        if pl_float < 0:
            pl = round(pl_float, 2)
        else: # tira comissão
            pl = round(pl_float * 0.935, 2)

        match_db.profit = pl
        session.commit()

        # telegram
        if pl > 0:
            MSG = f'**{match_db.name}** 💰🤑 R$ {pl}'
        else:
            MSG = f'**{match_db.name}** 😐❌ -R$ {pl}'
        telegram_id = enviar_no_telegram(chat_id=os.getenv('TELEGRAM_CHAT_ID'), msg=MSG)


analisa_jogos_em_andamento()
schedule.every(7).minutes.do(analisa_jogos_em_andamento)
schedule.every(30).seconds.do(atualizar_eventos_em_andamento)
schedule.every(10).seconds.do(monitorar_entradas)

# schedule.every(10).minutes.do(atualizar_pl)

while True:
    schedule.run_pending()
    time.sleep(1)


# payload = {
#     "jsonrpc": "2.0",
#     "method": "SportsAPING/v1.0/listCompetitions",
#     "params": {
#         "filter": {
#             "marketIds": ["1.256054306"]
#         }
#     },
#     "id": 1
# }
# payload = str(payload).replace("'", '"')
# o = callAping(jsonrpc_req=payload)