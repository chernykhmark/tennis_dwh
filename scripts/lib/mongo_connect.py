from dotenv import load_dotenv
load_dotenv()
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from contextlib import contextmanager


class MongoConnect:
    def __init__(self, uri: str = None, db_name: str = None):
        self.uri = uri or os.getenv('ATLAS_CONNECTION_STRING')
        self.db_name = db_name or os.getenv("ATLAS_DB_NAME")

    @contextmanager
    def get_db(self, collection_name: str = None):
        client = MongoClient(
            self.uri,
            server_api=ServerApi('1'),
            tls=True,
            tlsAllowInvalidCertificates=True # Разрешить невалидные сертификаты во время разработки
        )
        try:
            db = client[self.db_name]
            print("Успешное подключение к MongoDB")
            if collection_name:
                yield db[collection_name]
            else:
                yield db
        finally:
            client.close()


#