import json

class Signal:

    def __init__(self, signal=None):
        if signal is not None:
            self.signal = signal

    def get_measurements(self):
        return self.signal["measurements"]

    def id(self):
        return self.signal["_id"]

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

    def sensor_type(self):
        return self.signal["sensor_type"]

    def sensor_data_source(self):
        return self.signal["sensor_data_source"]

    def file_data_col_names(self):
        return self.signal["file_data_col_names"]

    def file_uri(self):
        return self.signal["file_uri"]

    def toJSON(self):
        return json.loads(json.dumps(self.signal))