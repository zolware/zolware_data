import requests
import json
from bson.objectid import ObjectId

from zolware_data.data import database
from zolware_data.models import signal
from zolware_data import config


class Datasource:

    def __init__(self, datasource=None):

        if datasource is not None:
            self.datasource = datasource

    def fetch(self, user, datasource_id):
        headers = {
            "content-type": "application/json",
            "Authorization": "Bearer " + user.token()
        }
        url = config.api_endpoint + '/datasources/' + datasource_id
        data = {}
        res = requests.get(url, data=data, headers=headers)
        if res.ok:
            self.datasource = res.json()['datasource']
        else:
            self.datasource = None

    def id(self):
        return self.datasource["_id"]

    def status(self):
        return self.datasource["status"]

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
