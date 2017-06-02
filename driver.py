import pprint

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
for signal in datasource.signals:
    print(signal.name)


datasource_reader = datasource_reader.DataSourceReader(datasource)
series = datasource_reader.read()
print(series.to_json())
print(series.loc['1980-01-02':'1980-01-04'])