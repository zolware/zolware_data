import requests
import boto3
import io
import smart_open

import pandas as pd

from zolware_data import config


class DataSourceReader:

    def __init__(self, datasource):
        self.datasource = datasource
        self.datetime_signal = 0
        if self.has_datestamp_signal() is False:
            raise RuntimeError("Warning no datestamp column")

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
        series = series.set_index([self.datasource.signals[self.datetime_signal].name])
        return series

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
            if signal.data_type == 'Timestamp':
                col_count = col_count + 1
                self.datetime_signal = col_count
                return True
        return False
