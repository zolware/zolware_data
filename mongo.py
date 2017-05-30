
import pprint
import pymongo

from pymongo import MongoClient

client = MongoClient('mongodb://quantifiedtrade:stimpy2305@ds045465.mlab.com:45465/quantifiedtrade_data')



db = client['quantifiedtrade_data']

datasources = db.data_sources

pprint.pprint(datasources.find_one())





