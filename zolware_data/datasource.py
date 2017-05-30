import pprint

from bson.objectid import ObjectId

from zolware_data import database


class Datasource:

    def __init__(self, datasource_id):
        self.db = database.Database().get_db()
        self.datasources = self.db.data_sources
        self.datasources.find_one({'_id': ObjectId(datasource_id)})
        pprint.pprint(self.datasources.find_one())



