import requests
from bson.objectid import ObjectId

from zolware_data.data import database
from zolware_data.models import signal
from zolware_data import config

class Datasource:

    def __init__(self, user):
        self.db = database.Database().get_db()
        self.datasources = self.db.data_sources
        self.datasource = None
        self.user = user

    def fetch(self, datasource_id):
        self.datasource = self.datasources.find_one({'_id': ObjectId(datasource_id)})

    def fetch2(self, datasource_id):
        headers = {
            "content-type": "text",
            "Authorization": "Bearer " + self.user.token()
        }
        url = config.api_endpoint + '/datasources/' + datasource_id

        self.datasource = requests.post(url, data=data, headers = headers)

    def name(self):
        return self.datasource["name"]

    def description(self):
        return self.datasource["description"]

    def num_signals(self):
        return len(self.datasource["signals"])

    def get_signals(self):
        signal_array = []
        for sig in self.datasource["signals"]:
            signalobject = Datasource.get_signal(sig)
            signal_array.append(signalobject)
        return signal_array

    @staticmethod
    def get_signal(signal_id):
        signal_id = ObjectId(signal_id)
        signalobject = signal.Signal(signal_id)
        return signalobject
