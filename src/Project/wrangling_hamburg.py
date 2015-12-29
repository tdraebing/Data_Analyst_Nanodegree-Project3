# -*- coding: utf-8 -*-

"""
Created on Mon Aug 31 15:06:18 2015

@author: Thomas Dräbing
"""
import createjson
import createdb
import audit_data
import db_ops
import pprint

def start(db_name, collection, osm_file, list_indices):
    #Create JSON file
    createjson.process_map(osm_file)
    
    #create DB
    db = db_ops.get_db(db_name)
    
    #load JSON into database
    createdb.insert_data(db, collection, osm_file + '.json')
    
    #create indices
    db_ops.create_indices(db, collection, list_indices)
    
    #get all distinct fields in database
    print(db_ops.get_fields(db, collection))
    
    #audit given fields
    audit_data.audit_fixme(db, collection)
    audit_data.audit_country(db, collection)
    audit_data.audit_state(db, collection)
    audit_data.audit_postcode(db, collection)
    audit_data.audit_city(db, collection)
    audit_data.audit_streets(db, collection)
    audit_data.audit_housenumbers(db, collection)
    
def queries(db_name, collection):
    
    #load DB
    db = db_ops.get_db(db_name)
    
    #queries as in sample project
    print('Number of documents: ' + str(db[collection].find().count()))
    print('Number of nodes: ' + str(db[collection].find({"type":"node"}).count()))
    print('Number of ways: ' + str(db[collection].find({"type":"way"}).count()))
    print('Number of relations: ' + str(db[collection].find({"type":"relation"}).count()))
    print('Number of unique users: ' + str(len(db[collection].distinct("created.user"))))
    pipeline = [{"$group":{"_id":"$created.user", "count":{"$sum":1}}}, 
            {"$group":{"_id":"$count", "num_users":{"$sum":1}}}, 
            {"$sort":{"_id":1}}, 
            {"$limit":1}]
    print('Number of users posted once: ' + str(db[collection].aggregate(pipeline)['result'][0]["num_users"]))
    
    #Own queries
    print('Number of pizza places: ' + str(db[collection].find({"cuisine":"pizza"}).count()))
    pipeline = [{'$group':{"_id":"$year", "count":{"$sum":1}}},
            {'$sort':{'count':-1}},
            {"$limit":10}]
    print('Top 10 building years: ')
    result = db[collection].aggregate(pipeline)['result']
    print('year\t\tcount\n')
    for r in result:
        print(str(r['_id']) + '\t\t' + str(r['count']))
        
if __name__ == '__main__':
    
    #CONFIGURATION
    #Please change the respective values for your needs
    #Be advised that the auditing functions are tailored for the hamburg region and may
    #need adjustment, when using them for other applications    
    
    db_name = 'osm'
    collection = 'hamburg'
    osm_file = 'Raw Data/hamburg_germany.osm'
    list_indices = ['address.city','address.street','address.housenumber']
    
    #start(db_name, collection, osm_file, list_indices)
    queries(db_name, collection)