from dotenv import load_dotenv
load_dotenv()
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


class MongoDB:
    def __init__(self):
        uri = os.getenv('ATLAS_CONNECTION_STRING')
        self.client = MongoClient(uri, server_api=ServerApi('1'))
        self.db_name = os.getenv("ATLAS_DB_NAME")
        self.db = self.client[self.db_name]
        print("Успешное подключение к MongoDB")

    def close(self):
        self.client.close()



