import pprint

from zolware_data import user_manager
from zolware_data import datasource_manager

from zolware_data import signal_data_reader

series = signal_data_reader.read_from_file('data.csv')

print(series.head())