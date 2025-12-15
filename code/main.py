from api_betfair import callAping
import logging
from datetime import datetime, timedelta, timezone
import json
import pandas as pd


from helper_db import engine, TblUnderSmash, session, StatusEnum
from sqlalchemy import select
from helpers import *
import platform

from dotenv import load_dotenv
load_dotenv()

ignorar_events = [] # add o id dos eventos que sairam do padrão

logging.basicConfig(
    # filename=
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

    list_events = json.loads(list_events)

    jogos_do_dia = []
    for jogo in list_events['result']:
        jogos_do_dia.append(jogo['event'])

    df_events = pd.DataFrame(jogos_do_dia)
        
    df_events['minuto_aproximado'] = df_events['openDate'].map(lambda x: minutos_aproximados(x)) # Quantos minutos aprox o jogo tem

    df_events = df_events.sort_values('openDate')
    df_events.rename(columns={"id": "event_id"}, inplace=True)

    df_events = df_events[["event_id", "name", "minuto_aproximado"]]
    logging.info("Jogos em andamento: %s", len(df_events))
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

        market_catalogue = json.loads(market_catalogue)

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
        except Exception as error:
            logging.error("INSERT: %s", str(error))
            pass

    # ignorar eventos que estão no banco
    for id in list(df['event_id']):
        if id not in ignorar_events: ignorar_events.append(id)


# Atualizar eventos que estão no banco
def atualizar_eventos_em_andamento():
    stmt = select(TblUnderSmash).where(TblUnderSmash.status == StatusEnum.IN_PLAY)
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
                row.status = StatusEnum.FINISH
            if status_over == "WINNER": # significa que saiu um gol ai nao posso atualizar mais a odd.
                row.dt_market_closed = datetime.now()
                row.status = StatusEnum.FINISH

        if status_market == "CLOSED":
            row.dt_market_closed = datetime.now()
            row.status = StatusEnum.FINISH
        
    session.commit()

import schedule
import time

analisa_jogos_em_andamento()
schedule.every(7).minutes.do(analisa_jogos_em_andamento)
schedule.every(30).seconds.do(atualizar_eventos_em_andamento)

while True:
    schedule.run_pending()
    time.sleep(1)