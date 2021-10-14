from typing import Optional

from sqlalchemy.engine import Engine, Connection

from config import SnowflakeConfig

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from baseclass import Base


class SnowflakeSQLAlchemyEngine:

    def __init__(self, user: str = SnowflakeConfig.USERNAME, password: str = SnowflakeConfig.PASSWORD,
                 account_identifier: str = SnowflakeConfig.ACCOUNT):
        self._conn_str = "snowflake://{user}:{password}@{account_identifier}/".format(
            user=user, password=password, account_identifier=account_identifier
        )

        self._engine: Optional[Engine] = None
        self._conn: Optional[Connection] = None

    def _create_engine(self):
        self._engine = create_engine(self._conn_str)

    @property
    def engine(self):
        if not self._engine:
            self._create_engine()

        return self._engine

    @property
    def connection(self):

        if not self._conn:
            if not self._engine:
                self._create_engine()
            self._conn = self._engine.connect()

        return self._conn

    def dispose_engine(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None

    def close_connection(self):
        if self._conn:
            self._conn.close()
            self._conn = None

Connection
connection = engine.connect()
print(type(connection))

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

try:
    results = connection.execute("select current_version()").fetchone()
    print(results[0])

finally:
    connection.close()
    engine.dispose()


def init_db():
    Base.metadata.create_all(engine, )


def get_session():
    return sessionmaker(autocommit=False, autoflush=False, bind=engine)
