from pandas import Series
import pandas as pd


def read_from_file(signal):
    # Make this safe by chcking columns etc
    names_in = signal.file_data_col_names().split(',')
    print(names_in)
    series = pd.read_csv(signal.file_uri(), sep=',', parse_dates=[0], header=0, names=names_in)
    series = series.set_index(['date'])
    return series
