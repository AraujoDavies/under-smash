from sqlalchemy import create_engine
import os

import logging
from sqlalchemy.orm import Session
from sqlalchemy import select
from sqlalchemy import Enum as SqlEnum
from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base
from enum import Enum

from dotenv import load_dotenv
load_dotenv()

engine = create_engine(os.getenv('DATABASE_URI')) # "mysql+pymysql://root:admin@localhost:3306/betfairApps"
session = Session(engine)
Base = declarative_base()


class StatusEnum(str, Enum):
    IN_PLAY = "in play"
    FINISH = "finish"


class TblUnderSmash(Base):
    __tablename__ = "under_smash"

    id = Column(Integer, primary_key=True, index=True)
    event_id = Column(Integer, unique=True)
    name = Column(String(255), nullable=False)
    placar = Column(String(10), nullable=False)
    mercado = Column(String(50), nullable=False)
    tempo = Column(Float, nullable=False)
    inPlayMatchStatus = Column(String(100))
    status = Column(SqlEnum(StatusEnum), nullable=False, default=StatusEnum.IN_PLAY)
    market_id = Column(String(50), nullable=False)
    lay_under = Column(Float, nullable=False)
    odd_max_saida = Column(Float, nullable=False)
    total_correspondido = Column(Float, nullable=False)
    dt_insert = Column(DateTime)
    dt_last_update_odd = Column(DateTime)
    dt_market_closed = Column(DateTime)

    def __repr__(self):
        return f"<User event_id={self.event_id} name={self.name}>"


if __name__ == '__main__':
    Base.metadata.create_all(engine)
