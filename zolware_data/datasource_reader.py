import requests
import json
import io
import smart_open

import pandas as pd

from zolware_data import config
from zolware_data import signal_manager


class DataSourceReader:

    def __init__(self, datasource, user):
        self.datasource = datasource
        self.user = user
        self.datetime_signal_col = 0
        self.datetime_signal_datetime = []
        self.datetime_signal_datacols = []
        if self.has_datestamp_signal() is False:
            raise RuntimeError("Warning no datestamp column")

        self.signal_map = {}
        for signal in self.datasource.signals:
            self.signal_map[signal.name] = signal
        print(self.signal_map)

    def read(self):
        if self.datasource.data_source == 'data_source_file':
            # Local, http, or AWS S3
            file_uri = self.datasource.file_uri
            if self.is_S3_url(file_uri):
                bucket = config.AWS_STORAGE_BUCKET_NAME
                aws_access_key_id = config.AWS_ACCESS_KEY_ID
                aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
                self.read_from_s3_bucket_by_url(bucket, 'Tamar.txt', aws_access_key_id, aws_secret_access_key)
            else:
                if self.url_exists(file_uri) is False:
                    self.datasource.status_msg = 'File URI ' + file_uri + ' does not exist'
                else:
                    return self.read_from_file()

    def read_from_s3_bucket(self, bucket, file):
        # s3 = boto3.resource(
        #     's3',
        #    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        #     aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
        # )
        # s3.bucket(bucket).download_file(file, dest)
        for line in smart_open.smart_open('s3://'+bucket+'/'+file):
            print(line)

    def read_from_s3_bucket_by_url(self, bucket, file, aws_access_key_id, aws_secret_access_key):
        s3url = 's3://'+aws_access_key_id+':'+aws_secret_access_key+'@'+bucket+'/'+file
        for line in smart_open.smart_open(s3url):
            print(line)

    def read_from_file(self):
        # Make this safe by chcking columns etc
        names_in = self.get_data_columns().split(",")
        file_line_cursor = self.datasource.file_line_cursor
        s = requests.get(self.datasource.file_uri).content
        series = pd.read_csv(io.StringIO(s.decode('utf-8')), sep=',',
                             parse_dates=[0], header=0, names=names_in,
                             skiprows=file_line_cursor)
        series = series.set_index([self.datasource.signals[self.datetime_signal_col].name])

        signal_man = signal_manager.SignalManager(self.user)

        for data_col in self.datetime_signal_datacols:
            num_data = len(series)
            json_object = json.loads(series.to_json(orient='split', date_format='iso'))
            temp_array = []
            #print(json_object['data'])
            #print(json_object['index'])
            #print(json_object['columns'])
            for x in range(0, len(json_object['columns'])):
                print(json_object['columns'][x])
                temp_array.append({"datetime": json_object['index'][x], "value": json_object['data'][x]})
                print(temp_array)

            signal_man.save_signal_data(self.signal_map[data_col], temp_array)

        return temp_array

    def url_exists(self, url):
        return requests.head(url).status_code == 200

    def is_S3_url(self, url):
        if url.startswith('s3://') or url.startswith('https://s3'):
            return True
        else:
            return False

    def get_data_columns(self):
        column_names = ""
        signals = self.datasource.signals
        for signal in signals:
            column_names = column_names + signal.name + ','
        print(column_names[:-1])
        return column_names[:-1]

    def has_datestamp_signal(self):
        signals = self.datasource.signals
        col_count = -1
        for signal in signals:
            col_count = col_count + 1
            if signal.data_type == 'Timestamp':
                self.datetime_signal_datetime.append(signal.name)
                self.datetime_signal_col = col_count
            else:
                self.datetime_signal_datacols.append(signal.name)
        if len(self.datetime_signal_datetime) > 0:
            return True
        else:
            return False
