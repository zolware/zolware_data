import requests
import json
from bson.objectid import ObjectId

from zolware_data.data import database
from zolware_data.models import signal
from zolware_data import config


class Datasource:

    def __init__(self, datasource=None):
        self.signals = []
        if datasource is not None:
            self.id = datasource["_id"]
            self.name = datasource["name"]
            self.description = datasource["description"]
            self.dt = datasource["dt"]
            self.file_line_cursor = datasource["file_line_cursor"]

            for signals in datasource["signals"]:
                self.signals.append(signal.Signal(signals))

            self.file_data_col_names = datasource["file_data_col_names"]
            self.file_uri = datasource["file_uri"]
            self.data_source = datasource["data_source"]

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
