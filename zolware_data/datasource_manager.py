import requests
from bson.objectid import ObjectId

from zolware_data.models import signal
from zolware_data.models import datasource
from zolware_data import config
from zolware_data.data import database


class DatasourceManager:

    def __init__(self, user):
        self.datasources = []
        self.user = user

    def get_datasource_by_id(self, datasource_id):
        headers = DatasourceManager.__construct_headers__(self.user)
        url = config.api_endpoint + '/datasources/' + datasource_id
        data = {}
        res = requests.get(url, data=data, headers=headers)
        if res.ok:
            return datasource.Datasource(res.json()['datasource'])
        else:
            return []

    def get_all_datasources(self):
        headers = DatasourceManager.__construct_headers__(self.user)
        url = config.api_endpoint + '/datasources'
        data = {}
        res = requests.get(url, data=data, headers=headers)
        if res.ok:
            datasources_return = []
            datasources = res.json()['datasources']
            for ds in datasources:
                datasources_return.append(datasource.Datasource(ds))
            return datasources_return
        else:
            return []

    def get_all_signals_for_datasource(self, datasource_id):
        headers = DatasourceManager.__construct_headers__(self.user)
        url = config.api_endpoint + '/datasources/' + datasource_id + '/signals'
        data = {}
        res = requests.get(url, data=data, headers=headers)
        if res.ok:
            signals_return = []
            signals = res.json()['signals']
            for sig in signals:
                signals_return.append(signal.Signal(sig))
            return signals_return
        else:
            return []


    @staticmethod
    def fetch_by_db(datasource_id):
        db = database.Database().get_db()
        datasources = db.data_sources
        return datasources.find_one({'_id': ObjectId(datasource_id)})

    @staticmethod
    def __construct_headers__(user):
        return {
            "content-type": "application/json",
            "Authorization": "Bearer " + user.token()
        }