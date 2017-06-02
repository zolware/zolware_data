from pymongo import MongoClient

from zolware_data import config


class Database:

    def __init__(self):
        config.db_password
        connect_string = 'mongodb://quantifiedtrade:'+config.db_password+'@ds045465.mlab.com:45465/quantifiedtrade_data'
        self.client = MongoClient(connect_string)

    def get_db(self, database_name):
        db = self.client[database_name]
        return db

    def get_db(self):
        db = self.client['quantifiedtrade_data']
        return db