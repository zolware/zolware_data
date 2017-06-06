import requests
import json
from bson.objectid import ObjectId

from zolware_data import signal_manager
from zolware_data.models import signal
from zolware_data import config


class Datasource:

    def __init__(self, user, datasource=None):
        self.signals = []
        self.user = user
        if datasource is not None:
            self.id = datasource["_id"]
            self.name = datasource["name"]
            self.description = datasource["description"]
            self.dt = datasource["dt"]
            self.file_line_cursor = datasource["file_line_cursor"]
            self.file_data_col_names = datasource["file_data_col_names"]
            self.file_uri = datasource["file_uri"]
            self.data_source = datasource["data_source"]
            self.status = datasource["status"]

    def fetch(self, user, datasource_id):
        headers = self.__construct_headers__()
        url = config.api_endpoint + '/datasources/' + datasource_id
        data = {}
        res = requests.get(url, data=data, headers=headers)
        if res.ok:
            self.datasource = res.json()['datasource']
        else:
            self.datasource = None

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

    def populate_signals(self):
        headers = self.__construct_headers__()
        url = config.api_endpoint + '/datasources/' + self.id + '/signals'
        data = {}
        res = requests.get(url, data=data, headers=headers)
        if res.ok:
            signals = res.json()['signals']
            for sig in signals:
                self.signals.append(signal.Signal(sig))
        else:
            print(res.status_code)
            return []

    def __construct_headers__(self):
        return {
            "content-type": "application/json",
            "Authorization": "Bearer " + self.user.token
        }
