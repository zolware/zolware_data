import requests
import json
import io
import smart_open
import pprint

import pandas as pd

from zolware_data import config
from zolware_data import datasource_manager
from zolware_data.utils import url_util


class DataSourceReader:

    def __init__(self, datasource, user):
        self.datasource = datasource
        self.user = user
        self.datetime_signal_col = 0
        self.datetime_signal_datetime = []
        self.datetime_signal_datacols = []

        self.signal_map = {}
        for signal in self.datasource.signals:
            self.signal_map[signal.name] = signal

    def read(self):
        if self.datasource.data_source == 'data_source_file':
            # Local, http, or AWS S3
            file_uri = self.datasource.file_uri
            if url_util.is_S3_url(file_uri):
                bucket = config.AWS_STORAGE_BUCKET_NAME
                aws_access_key_id = config.AWS_ACCESS_KEY_ID
                aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY
                self.read_from_s3_bucket_by_url(bucket, 'Tamar.txt', aws_access_key_id, aws_secret_access_key)
            else:
                if url_util.url_exists(file_uri) is False:
                    self.datasource.status_msg = 'File URI ' + file_uri + ' does not exist'
                    raise RuntimeError('File URI ' + file_uri + ' does not exist')
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
        # Get columnn names from the names of the signals
        names_in = self.get_data_columns().split(",")
        # Get the position all ready read in the datasource
        file_line_cursor = self.datasource.file_line_cursor

        file_handle = requests.get(self.datasource.file_uri).content
        file_content = io.StringIO(file_handle.decode('utf-8'))
        first_line = file_content.readline()
        tokens_in_file = first_line.split(",")
        if len(tokens_in_file) != len(names_in):
            raise RuntimeError('Number of signals specified not equal to number of columns in file.')

        series = pd.read_csv(file_content, sep=',',
                             parse_dates=[0], names=names_in,
                             skiprows=file_line_cursor)
        # Set the series index to the name of the datestamp column (first found)
        series = series.set_index([self.datetime_signal_datetime[0]])
        # Get the signal manager
        datasource_man = datasource_manager.DatasourceManager(self.user)
        # Loop over the data columns (not the datestamp colum)
        json_object = json.loads(series.to_json(orient='split', date_format='iso'))

        temp_array = []
        # Loop over the number
        signal_data = [[0 for i in range(len(json_object['data']))] for j in range(len(self.datetime_signal_datacols))]
        for x in range(len(json_object['data'])):
            datetime = json_object['index'][x]
            values = json_object['data'][x]
            for s in range(len(self.datetime_signal_datacols)):
                signal_data[s][x] = {"datetime": datetime, "value": values[s]}

        tdata = []
        for x in range(len(json_object['data'])):
            tdata.append({"datetime": json_object['index'][x], "values": json_object['data'][x]})

        datasource_man.save_measurement_data(self.datasource.id, tdata)

        return temp_array

    def get_data_columns(self):
        col_count = 0
        column_names = ""
        signals = self.datasource.signals
        for signal in signals:
            col_count = col_count + 1
            column_names = column_names + signal.name + ','
            if signal.data_type == 'Timestamp':
                self.datetime_signal_datetime.append(signal.name)
                self.datetime_signal_col = col_count
            else:
                self.datetime_signal_datacols.append(signal.name)
        if len(self.datetime_signal_datetime) < 1:
            raise RuntimeError('No datetime column defined for signal ' + signal.name)
        return column_names[:-1]
