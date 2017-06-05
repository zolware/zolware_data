import json


class Signal:

    def __init__(self, signal=None):
        if signal is not None:
            self.signal = signal
            print(signal)
            self.id = signal["_id"]
            self.name = signal["name"]
            self.description = signal["description"]
            self.x_max = signal["Xrange"]["max"]
            self.x_min = signal["Xrange"]["min"]
            self.y_max = signal["Yrange"]["max"]
            self.y_min = signal["Yrange"]["min"]
            self.data_count = signal["data_count"]
            self.data_type = signal["data_type"]
            self.sensor_type = signal["sensor_type"]
            self.status_msg = signal["status_msg"]

    def get_measurements(self):
        return self.signal["measurements"]

    def id(self):
        return self.id

    def name(self):
        return self.name

    def description(self):
        return self.description

    def x_max(self):
        return self.x_max

    def x_min(self):
        return self.x_min

    def y_max(self):
        return self.y_max

    def y_min(self):
        return self.y_min

    def data_count(self):
        return self.data_count

    def dt(self):
        return self.dt

    def sensor_type(self):
        return self.sensor_type


    def toJSON(self):
        return json.loads(json.dumps(self.__dict__))
