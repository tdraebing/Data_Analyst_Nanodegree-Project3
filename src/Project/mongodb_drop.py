# dropping a database via pymongo
from pymongo import Connection
c = Connection()
c.drop_database('osm')

# drop a collection via pymongo
#from pymongo import Connection
#c = Connection()
#c['hamburg'].drop_collection('mycollection')
