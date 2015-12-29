###############################################################
#Script to load data from a JSON file into a MongoDB database.#
###############################################################

import json

#function that facilitates the insertion of the data stored in a JSON file into the database    
def insert_data(db, db_name, json_file):
    with open(json_file) as f:
        for line in f:
            data = json.loads(line)
            db[db_name].insert(data)
        
