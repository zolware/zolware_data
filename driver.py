import pprint

from zolware_data.models import datasource

datasource = datasource.Datasource()
datasource.fetch('59287a6d68ca556dbe4f6fd6')

print(datasource.name())
print(datasource.num_signals())


signals = datasource.get_signals()
for sig in signals:
    pprint.pprint(sig)
    print(sig.x_max())
