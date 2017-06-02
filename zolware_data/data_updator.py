import pprint

from zolware_data import user_manager
from zolware_data import datasource_manager
from zolware_data import signal_manager
from zolware_data import datasource_reader

user_manager = user_manager.UserManager()
user = user_manager.find_user_by_email('snclucas@gmail.com')
datasource_manager = datasource_manager.DatasourceManager(user)
signal_manager = signal_manager.SignalManager(user)



# Loop over datasources
data_sources = datasource_manager.get_all_datasources();
for datasource in data_sources:
    if datasource.status() == 'true':
        print('----------------------')
        print('Datasource: ' + datasource.name())
        datasource_reader = datasource_reader.DataSourceReader(datasource, user)
        series = datasource_reader.read()

