from pymongo import MongoClient
from bson.code import Code
import re

#function to connect to the MongoDB
def get_db(db_name):
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db
    
#function to index data in the database (multiple single field indexes)            
def create_indices(db, collection, fields):
    for field in fields:
        db[collection].create_index(field)
        
#function to retrieve the names of all fields in the database
def get_fields(db, collection):
    fields = []    
    #map_reduce is used to retrieve the keys
    reduce = Code("function(key, stuff) { return null; }")
    map = Code("function() {for (var key in this) { emit(key, null); }}")
    mr = db[collection].map_reduce(map,  reduce, collection + "_keys")
    for doc in mr.find():
        fields.append(doc['_id'])
    #going into nested fields. should later be done recursively, if other nested fields can be found in the database  
    map_address = Code("function() {for (var key in this.address) { emit(key, null); }}")
    mr_address = db[collection].map_reduce(map_address,  reduce, collection + "_keys")
    for doc in mr_address.find():
        fields.append('address.' + doc['_id'])    
        
    map_created = Code("function() {for (var key in this.created) { emit(key, null); }}")
    mr_created = db[collection].map_reduce(map_created,  reduce, collection + "_keys")
    for doc in mr_created.find():
        fields.append('created.' + doc['_id'])
    #saving feld names to file
    with open('Output\\' + collection + '_fields.txt', 'w') as f:
        for field in fields:
            f.write(unicode(field + '\n').encode("utf-8"))
            
    return(len(fields))
    
#This function searches the values for certain characters to filter possible unclean entries    
def key_type(data):
	#The code checks whether the k-attribute in the tag-tag contains elements that are described by the regular expressions above and increments the respective counts in the keys-dictionary.
    keys = keys = {"lower": 0, "lower_colon": 0,'space': 0, "problemchars": 0, "numbers": 0}
    lower = re.compile(r'^([a-z]|_)*$')
    lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
    space = re.compile(r'[ ]')
    numbers = re.compile(r'[0-9]')
    problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\.\t\r\n]')
    for k in data:
        if lower.search(k) != None:
            keys['lower'] = keys['lower'] + 1
        if lower_colon.search(k) != None:
            keys['lower_colon'] = keys['lower_colon'] + 1
        if problemchars.search(k) != None:
            keys['problemchars'] = keys['problemchars'] + 1
            #print(k)
        #Spaces were one of the most common problemchars. But they belong into the street names and thus do not indicate bad entries
        if space.search(k) != None:
            keys['space'] = keys['space'] + 1  
        if numbers.search(k) != None:
            keys['numbers'] = keys['numbers'] + 1
    return keys
    
#Function to retrieve the values stored in given fields. Fields have to be given as list.    
def get_values(db, collection, fields):
    values = []
    for field in fields:
        values = values + db[collection].distinct(field)
    with open(collection + '_' + field+ '.txt', 'w') as f:
        for value in values:
            f.write(unicode(value + '\n').encode("utf-8"))
    print(key_type(values))
    return values

#Function to update multiple given values of a certain field    
def update_db(db, collection, field, new_values):
    for i in new_values:
        for k in new_values[i]:
            db[collection].update({field:k},{'$set':{field:new_values[i][k]}})

#Function to move certain documents to another collection            
def move_db(db, collection, new_collection, field, value):
    # Moves all documents possesing the given fielf
    if value == None:
        for record in db[collection].find({field : {'$exists' : 'true'}}):
            db[new_collection].insert(record)
        db[collection].remove({field : {'$exists' : 'true'}})
    # Documents to move have to have the given field with the given value
    else:
        for record in db[collection].find({field:value}):
            db[new_collection].insert(record)
        db[collection].remove({ field : value })