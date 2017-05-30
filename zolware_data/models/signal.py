import pprint

from bson.objectid import ObjectId

from zolware_data.data import database


class Signal:

    def __init__(self, signal_id):
        self.db = database.Database().get_db()
        self.signals = self.db.signals
        self.signal = self.signals.find_one({'_id': ObjectId(signal_id)})
        pprint.pprint(self.signal)

    def get_measurements(self):
        return self.signal["measurements"]

    def name(self):
        return self.signal["name"]

    def description(self):
        return self.signal["description"]

    def x_max(self):
        return self.signal["Xrange"]["max"]

    def x_min(self):
        return self.signal["Xrange"]["in"]

    def y_max(self):
        return self.signal["Yrange"]["max"]

    def y_min(self):
        return self.signal["Yrange"]["in"]

    def data_count(self):
        return self.signal["data_count"]

    def dt(self):
        return self.signal["dt"]