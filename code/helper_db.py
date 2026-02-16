from sqlalchemy import create_engine
import os
import pandas as pd

from sqlalchemy.orm import Session
from sqlalchemy import Enum as SqlEnum
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, text
from sqlalchemy.orm import declarative_base
from enum import Enum


from dotenv import load_dotenv
load_dotenv()

engine = create_engine(os.getenv('DATABASE_URI')) # "mysql+pymysql://root:admin@localhost:3306/betfairApps"
session = Session(engine)
Base = declarative_base()


class TblUnderSmash(Base):
    __tablename__ = "under_smash"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, unique=True)
    name = Column(String(255), nullable=False)
    placar = Column(String(10), nullable=False)
    mercado = Column(String(50), nullable=False)
    tempo = Column(Float, nullable=False)
    inPlayMatchStatus = Column(String(100))
    status = Column(String(45), nullable=False, default="IN_PLAY")
    market_id = Column(String(50), nullable=False)
    lay_under = Column(Float, nullable=False)
    odd_max_saida = Column(Float, nullable=False)
    total_correspondido = Column(Float, nullable=False)
    dt_insert = Column(DateTime)
    dt_last_update_odd = Column(DateTime)
    dt_market_closed = Column(DateTime)
    # processo de entrada no mercado
    selection_id = Column(String(50))
    stake = Column(Float, server_default="0", nullable=False)
    profit = Column(Float, server_default="0", nullable=False)
    dt_entrada = Column(DateTime)
    betfair_response_entrada = Column(Text)
    betfair_response_saida = Column(Text)

    def __repr__(self):
        return f"<User event_id={self.event_id} name={self.name}>"


def resumo_telegram():
    stmt = """SELECT 
        strftime('%m/%Y', dt_insert) AS month,
        sum(profit) as pl,
        count(1) as entradas,
        SUM(CASE WHEN profit > 0 THEN 1 ELSE 0 END) as greens,
        SUM(CASE WHEN profit < 0 THEN 1 ELSE 0 END) as reds,
        AVG(stake) as stake_media
    FROM under_smash
    WHERE stake > 0
    GROUP BY strftime('%m/%Y', dt_insert)
    ;"""
    with engine.begin() as c:
        result = c.execute(text(stmt)).fetchall()

    df = pd.DataFrame(result)

    df['pl'] = df['pl'].map(lambda x: f"R$ {round(x, 2)}" if x > 0 else f"-R$ {str(round(x, 2)).replace('-', '')}") # incluindo R$
    df['stake_media'] = df['stake_media'].map(lambda x: f"R$ {round(x, 2)}" if x > 0 else f"-R$ {str(round(x, 2)).replace('-', '')}") # incluindo R$

    resumo_mes = "\n".join(
        f"**{row[0]}** | {row[5]} | **{row[1]}** ({row[3]}G, {row[4]}R)"
        for row in df.itertuples(index=False)
    )

    MSG = f"**Daronco** \n\n<u>Resultado Mensal:</u> \n\n............| stake med. | PL\n{resumo_mes}"
    return MSG

if __name__ == '__main__':
    Base.metadata.create_all(engine)
