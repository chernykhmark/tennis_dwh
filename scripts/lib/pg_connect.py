import os
from dotenv import load_dotenv
load_dotenv()
from contextlib import contextmanager
from typing import Generator
import psycopg


class PgConnect:
    def __init__(self, host: str, port: str, db_name: str, user: str, pw: str) -> None:
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw,)

    def client(self):
        return psycopg.connect(self.url())

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        conn = psycopg.connect(self.url())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()


class ConnectionBuilder:

    @staticmethod
    def pg_conn() -> PgConnect:

        pg = PgConnect(os.getenv("PG_HOST"),
                       os.getenv("PG_PORT"),
                       os.getenv("PG_DB_NAME"),
                       os.getenv("PG_USER"),
                       os.getenv("PG_PASSWORD")
                       )
        return pg

