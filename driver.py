import pprint
import json
import pandas as pd
from zolware_data import user_manager
from zolware_data import datasource_manager

from zolware_data import user_manager
from zolware_data import datasource_manager
from zolware_data import signal_manager
from zolware_data import datasource_reader

user_manager = user_manager.UserManager()
user = user_manager.find_user_by_email('snclucas@gmail.com')
datasource_manager = datasource_manager.DatasourceManager(user)

datasource = datasource_manager.get_datasource_by_id('593171e75ef70532507d87d7')


datasource_reader = datasource_reader.DataSourceReader(datasource, user)
series = datasource_reader.read()

