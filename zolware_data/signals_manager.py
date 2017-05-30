import pprint

from zolware_data import user_manager
from zolware_data.models import datasource

user_manager = user_manager.UserManager()

user = user_manager.find_user_by_email('snclucas@gmail.com')

datasource = datasource.Datasource(user)

datasource.fetch('59287a6d68ca556dbe4f6fd6')

print(user.token())
pprint.pprint(datasource.name())

#Loop over users

#loop over datasources





