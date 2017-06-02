import requests
import pprint
import json

from zolware_data.models import signal
from zolware_data import config


class SignalManager:

    def __init__(self, user):
        self.datasources = []
        self.user = user

    def save_signal(self, signal_to_save):
        headers = SignalManager.__construct_headers__(self.user)
        url = config.api_endpoint + '/signals/' + signal_to_save.id
        data = signal_to_save.toJSON()
        data_json = json.dumps(data)
        res = requests.post(url, data=data_json, headers=headers)
        if res.ok:
            print(res.json())
        else:
            print(res.json())

    def get_signal_by_id(self, signal_id):
        headers = SignalManager.__construct_headers__(self.user)
        url = config.api_endpoint + '/signals/' + signal_id
        data = {}
        res = requests.get(url, data=data, headers=headers)
        if res.ok:
            return signal.Signal(res.json()['signal'])
        else:
            return []

    def get_all_signals(self):
        headers = SignalManager.__construct_headers__(self.user)
        url = config.api_endpoint + '/signals'
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
    def __construct_headers__(user):
        return {
            "content-type": "application/json",
            "Authorization": "Bearer " + user.token()
        }