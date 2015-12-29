#!/usr/bin/python
# -*- coding: utf-8 -*-

###################################################
#Script to clean the hamburg.osm data in a MongoDB#
###################################################

import pprint
import re
import json
import urllib
import db_ops
import time


def audit_fixme(db, collection):
    keys = ['fixme','FIXME', 'FIXME2',  'fixme:de', 'note:FIXME','source:fixme']
    for key in keys[1:]:
        db[collection].update({}, {'$rename':{key:"fixme"}})
    db_ops.move_db(db, collection, 'NeedsFix', 'fixme', None)
    

def audit_country(db, collection):
    countries = db_ops.get_values(db, collection, ['address.country'])
    for country in countries:
        print(country)
        #all entries are 'DE', thus no cleaning is required
        
def audit_state(db, collection):
    states = db_ops.get_values(db, collection, ['address.state'])
    # One Document had DE as state. validation showed that Schleswig-Holstein is the correct value
    #Be careful if using different data sets
    cor_state = {'manual':{'DE':'Schleswig-Holstein', 'NI':'Niedersachsen', 'Lower Saxony':'Niedersachsen', 'HH':'Hamburg'}}
    db_ops.update_db(db, collection, 'address.state', cor_state)    
    states = db_ops.get_values(db, collection, ['address.state'])
    for state in states:
        if state != 'Hamburg':
            db_ops.move_db(db, collection, 'SurroundingStates', 'address.state', state)

def audit_postcode(db, collection):
    pcs = db_ops.get_values(db, collection, ['address.postcode'])
    for pc in pcs:
        #One given postcode did not exist:
        if pc == '22701':
            pc = '22765' #found at google maps
            db[collection].update({'address.postcode':'22701'},{'$set':{'address.postcode':pc}})
        #postcodes and states were crossvalidated with the google maps API
        url = 'http://maps.googleapis.com/maps/api/geocode/json?address=urlencode($_REQUEST[' + pc + ' Germany])&sensor=false'
        attempts = 0
        success = False
        while success != True and attempts < 3:
            page = urllib.urlopen(url)
            data = json.loads(page.read())
            attempts += 1
            
            if data['status'] == "OVER_QUERY_LIMIT":
                time.sleep(2)
                continue
            success = True

        if attempts == 3:
            print("Daily limit has been reached")

        for i in range(len(data['results'][0]['address_components'])):
            if data['results'][0]['address_components'][i]['long_name'] in ['Schleswig-Holstein', 'Niedersachsen','Hamburg', 'Lower Saxony']:
                state = data['results'][0]['address_components'][i]['long_name']
                if state == 'Lower Saxony':
                    state = 'Niedersachsen'
        #Set the state field, if not already given
        if db[collection].find({'address.postcode':pc, 'address.state' : {'$exists' : 'false'}}) != None:
            db[collection].update({'address.postcode':pc},{'$set':{'address.state':state}})
        if state != 'Hamburg':
            db_ops.move_db(db, collection, 'SurroundingStates', 'address.postcode', pc)

def audit_city(db, collection):
    cities = db_ops.get_values(db, collection, ['address.city'])
    #clean city names shown to have problem characters
    cities_to_clean = {'wrong_state': {'Barendorf, Kreis Lüneburg':'Barendorf',
                                        'Wintermoor a. d. Ch.':'Wintermoor an der Chaussee',
                                        'Moisburg/Hollenstedt':'Moisburg',
                                        'Lauenburg/Elbe':'Lauenburg Elbe'}}
    db_ops.update_db(db, collection, 'address.city', cities_to_clean)
    #cross calidate cities with state and postal codes
    for city in cities:
        url = 'http://maps.googleapis.com/maps/api/geocode/json?address=urlencode($_REQUEST["' + city.encode('utf8') + '" Germany])&sensor=false&amp;oe=utf-8'
        attempts = 0
        success = False
        while success != True and attempts < 3:
            page = urllib.urlopen(url)
            data = json.loads(page.read())
            attempts += 1
            
            if data['status'] == "OVER_QUERY_LIMIT":
                time.sleep(2)
                continue
            success = True

        if attempts == 3:
            print("Daily limit has been reached")
        if data['status'] == 'ZERO_RESULTS':
            #print(city + ' not found!')
            with open('Output\\' + collection + '_unknown-cities.txt', 'a') as f:
                f.write(unicode(city + '\n').encode("utf-8"))
        else:
            try:
                for i in range(len(data['results'][0]['address_components'])):
                    if data['results'][0]['address_components'][i]['types'] == ['postal_code']:
                        pc = data['results'][0]['address_components'][i]['long_name']
                        if db[collection].find({'address.city':city, 'address.postcode':{'$ne':pc}}) != None:
                            print(city, pc)
                            db[collection].update({'address.city':city, 'address.postcode':{'$ne':pc}},{'$set':{'address.postcode':pc}})
                    if data['results'][0]['address_components'][i]['types'] == [ "administrative_area_level_1", "political" ]:
                        state = data['results'][0]['address_components'][i]['long_name']
                        if state != 'Hamburg':
                            db_ops.move_db(db, collection, 'SurroundingStates', 'address.city', city)
            except:
                pprint.pprint(data)
                
def audit_streets(db, collection):
    streets = db_ops.get_values(db, collection, ['address.street'])
    #some streets contain possible problem characters, but most are part of the Name
    #The others will be updated here
    streets_to_clean = {'1': {'Kandinskyallee, Ecke Feiningerstraße':'Kandinskyallee',
                             'I. Sandereiweg':'Erster Sandereiweg',
                             'II. Sandereiweg':'Zweiter Sandereiweg',
                             'III. Sandereiweg':'Dritter Sandereiweg'}}
    db_ops.update_db(db, collection, 'address.street', streets_to_clean)
    for street in streets:
        t = re.compile('[s|S]tr\.*$').search(street)
        if t != None:
            print(street)
            #No abbreviated streets were found
    
def audit_housenumbers(db, collection):
    hns = db_ops.get_values(db, collection, ['address.housenumber'])
    unclean_hns = []
    #List of non-existing house numbers
    entries_to_remove = ['g50','g70','g30a']
    #Dictionary of house numbers, that were cleaned manually, since their scheme
    #was rather rare
    manual = {'2-Block D':'2, Block D', 
              '1a-1':'1, 1A',
              '49 - 2.OG':'49',
              '71, Tor 2':'71',
              '37 (links)':'37',
              '44<B':'44B',
              '38-':'38',
              '7a b':'7A, B',
              '38 und a b':'38, 38A, B',
              '31 bc':'31B, C',
              '7 abc':'7A, B, C',
              '6a/b 4a/b':'4A, B, 6A, B',
              '48+a, 51,52':'48, 48A, 51, 52',
              '57, 59a + b, 61a + b':'57, 59A, B, 61A, B',
              '18 + a, 20 + a':'18, 18A, 20, 20A',
              '37b-39':'37B-39',
              '2 a b':'2A,B',
              '103/105/105a':'103,105,105A',
              '51a-e; 53 a-e; 55 a-e':'51A-E, 53A-E, 55A-E',
              '45, 47 a/b':'45, 47A, B',
              '41a (Tal)': '41A',
              '36, 36a - 36f':'36, 36A-F',
              '32;34:36':'32, 34-36',
              '10b-12':'10B-12',
              '17anb.':'17',
              '10 - 10a - 12':'10, 10A, 12'}
              
    #Dictionary in the form the db_ops.update_db function needs it. The numbers represent the
    #regular expression which detected the respective house number to be able to debug 
    #possble wrong changes
    cleaned = {'1':{},
               '2':{}, 
               '3':{},
               '4':{}, 
               '5': {}, 
               '6': {}, 
               '7': {}, 
               '8': {},
               '9': {},
               '10': {},
               '11': {},
               '12': {}, 
               '13': {}, 
               '14': {}, 
               '15': {}, 
               '16': {}, 
               '17': {}, 
               '18': {},  
               '19': {},  
               '20': {},  
               '21': {},   
               '22': {},  
               'manual': {},
               'remove': {},
               'not edited': {}}
               
    for hn in hns:
        #e.g. 3 // 3 Hof      =>       3 // 3 Hof
        if re.compile('((^[0-9]+$)|(^[0-9]+\s?Hof))').search(hn) != None:
            cleaned['1'][hn] = hn
        #e.g. 3a // 3 a // 3-a // 3 - a // 3 und a      =>     3A
        elif re.compile('^[0-9]+(\s|(\s?[-]\s?)|(\s?und\s?))?[a-zA-Z]$').search(hn) != None:
            num = re.compile('^[0-9]+').search(hn).group(0)
            char = re.compile('[A-Za-z]+$').search(hn).group(0)
            new_hn = num + char.upper()
            cleaned['2'][hn] = new_hn
        #e.g. 3/3 // 3 / 3      =>       3, Haus 3
        elif re.compile('^[0-9]+\s?[/]\s?[0-9]+$').search(hn) != None:
            nums = re.compile('[0-9]+').findall(hn)
            if abs(int(nums[0])-int(nums[1])) > 3:
                new_hn = nums[0] + ', Haus ' + nums[1]
            else:
                nums = sorted(nums)
                new_hn = nums[0]+ ', '+ nums[1]
            cleaned['3'][hn] = new_hn
        #e.g. 3,4 // 3 + 4      =>    3, 4
        elif re.compile('^[0-9]+\s?([,;+]\s?[0-9]+)+$').search(hn) != None:
            nums = re.compile('[0-9]+').findall(hn)
            new_hn = ', '.join(sorted([num for num in nums]))
            cleaned['4'][hn] = new_hn
        #e.g. 3 - 4 // 3 - 4a     =>     3-4 // 3-4A
        elif re.compile('^[0-9]+\s?([-:]\s?[0-9]+([a-zA-Z]+)?)+$', re.UNICODE).search(hn) != None:
            nums = re.compile('[0-9]+').findall(hn)
            sl = sorted([int(num) for num in nums])
            sl = [str(x).encode('utf-8') for x in sl]
            try:
                chars = re.compile('[A-Za-z]+').search(hn).group(0)
                new_hn = '-'.join(sl) + chars.upper()
            except:
                new_hn = '-'.join(sl)
            cleaned['5'][hn] = new_hn
        #e.g. 3 a-b // 3-a-b    => 3A-B
        elif re.compile('^[0-9]+(\s|[-])?[a-zA-Z]+(\s?[-]\s?[a-zA-Z]+)+$').search(hn) != None:
            num = re.compile('[0-9]+').search(hn).group(0)
            chars = re.compile('[A-Za-z]+').findall(hn)
            new_hn = num + '-'.join(sorted([char.upper() for char in chars]))
            cleaned['6'][hn] = new_hn
        #e.g. 3a,b,c // 3 a + b + c      => 3A,B,C
        elif re.compile('^[0-9]+\s?[a-zA-Z]?(\s?[,;/+]\s?[a-zA-Z]?)+$').search(hn) != None:
            num = re.compile('[0-9]+').search(hn).group(0)
            chars = re.compile('[A-Za-z]+').findall(hn)
            new_hn = num  + ','.join(sorted([char.upper() for char in chars]))
            cleaned['7'][hn] = new_hn
        #e.g. 3a,3b // 3a,4a   => 3A,B //3A, 4A
        elif re.compile('^[0-9]+\s?[a-zA-Z](\s?[,;/+&]\s?[0-9]+\s?[a-zA-Z]+)+$').search(hn) != None:
            el = re.split('[,;/+&]', hn)
            while '' in el:
                el.remove('')
            num = {}
            new_hn = ''
            for e in el:
                n = re.compile('[0-9]+').search(e).group(0)
                c = re.compile('[A-Za-z]+').search(e).group(0)
                try:
                    num[n].add(c)
                except:
                    num[n] = set(c)     
            for n in num:
                new_hn = new_hn + n  + ','.join(sorted([char.upper() for char in num[n]])) + '; '
            new_hn = new_hn[:-2]
            cleaned['8'][hn] = new_hn
        #e.g. 3a - 4b   =>   3A-4B
        elif re.compile('^[0-9]+\s?[a-zA-Z]+(\s?[-]\s?([0-9]+)\s?[a-zA-Z]+)+$').search(hn) != None:
            el = re.split('-', hn)
            num = {}
            new_hn = ''
            for e in el:
                n = re.compile('[0-9]+').search(e).group(0)
                c = re.compile('[A-Za-z]+').search(e).group(0)
                try:
                    num[n].add(c)
                except:
                    num[n] = set(c)     
            for n in num:
                new_hn = new_hn + n  + '-'.join(sorted([char.upper() for char in num[n]])) + ', '
            new_hn = new_hn[:-2]
            cleaned['9'][hn] = new_hn
        #e.g. 3a 4b 5c => 3A, 4B, 5C
        elif re.compile('^[0-9]+[a-zA-Z]+(\s[0-9]+[a-zA-Z]+)+$').search(hn) != None:
            el = re.split(' ', hn)
            num = {}
            new_hn = ''
            for e in el:
                n = re.compile('[0-9]+').search(e).group(0)
                c = re.compile('[A-Za-z]+').search(e).group(0)
                try:
                    num[n].add(c)
                except:
                    num[n] = set(c)     
            for n in num:
                new_hn = new_hn + n  + ', '.join(sorted([char.upper() for char in num[n]])) + ', '
            new_hn = new_hn[:-2]
            cleaned['10'][hn] = new_hn
        #e.g. 3 Haus 5a   => 3, Haus 5A
        elif re.compile('^[0-9]+\sHaus\s([0-9]+)?[a-zA-Z]?$').search(hn) != None:
            e = re.split(' ', hn)
            new_hn = e[0] + ', Haus ' + e[2].upper()
            cleaned['11'][hn] = new_hn
        #e.g. 3-4 Hs. 9  => 3-4, Haus 9 
        elif re.compile('^[0-9]+([-][0-9]+|[a-z])?\s?((Hs\.)|(HTH)|([.]))\s?[0-9]?\+?[0-9]?$').search(hn) != None:    
            e = re.split('\.?\s?', hn)
            if len(e) == 2:
                new_hn = e[0] + ', Haus ' + e[1]
            elif not e[2] == '':
                if re.compile('[a-z]+').search(e[0]) != None:
                    new_hn = e[0].upper() + ', Haus ' + e[2]
                else:
                    new_hn = '-'.join(re.split('-',e[0])).upper() + ', Haus ' + e[2]
            else:
                new_hn = '-'.join(re.split('-',e[0]))
            cleaned['12'][hn] = new_hn
        #e.g. 3 A4   =>   3, Haus A4
        elif re.compile('^[0-9]+\sA\s?[-]?[0-9]+$').search(hn) != None:    
            e = re.split('[ -]', hn)
            new_hn = e[0] + ', Haus A' + e[2]
            cleaned['13'][hn] = new_hn
        #e.g. 3, 3a, 3b    => 3,3A,B    
        elif re.compile('^[0-9]+\s?([,;/+]\s?[0-9]+\s?[a-zA-Z]+)+$').search(hn) != None:
            num = re.compile('[0-9]+').search(hn).group(0)
            chars = re.compile('[A-Za-z]+').findall(hn)
            new_hn = num  + ', ' + num + ','.join(sorted([char.upper() for char in chars]))
            cleaned['14'][hn] = new_hn
        #e.g. 3?    =>   3
        elif re.compile('[?]').search(hn) != None:
            new_hn = re.compile('[0-9]+').search(hn).group(0)
            cleaned['15'][hn] = new_hn
        #e.g. 3a und 3b  =>  3A,B
        elif re.compile('^[0-9]+[a-zA-Z]?(\s|(\s?und\s?))[0-9]+[a-zA-Z]?$').search(hn) != None:
            e = sorted(re.split(' ', hn))
            new_hn = e[0].upper() + ', ' + e[1].upper()
            cleaned['16'][hn] = new_hn
        #e.g. 3a/b    =>    3A,B
        elif re.compile('^[0-9]+[a-zA-Z][/]?[0-9]+$').search(hn) != None:
            nums = re.compile('[0-9]+').findall(hn)
            char = re.compile('[A-Za-z]+').search(hn).group(0)
            new_hn = nums[0] + char.upper() +', Haus ' + nums[1]
            cleaned['17'][hn] = None
        #e.g. T3    => 3
        elif re.compile('^T[0-9]+').search(hn) != None:
            cleaned['18'][hn] = re.compile('[0-9]+').search(hn).group(0)
        #e.g. 3a,3,3b  => 3,3A,3B
        elif re.compile('^([0-9]+[a-zA-Z]?[,]?)+$').search(hn) != None:
            n = re.split(',', hn)
            new_hn = ', '.join(sorted([x.upper() for x in n]))
            cleaned['19'][hn] = None
        #3,3a-f   =>   3, 3A-F
        elif re.compile('^[0-9]+[,+]\s?[0-9]+[a-zA-Z][-/][a-zA-Z]$').search(hn) != None:
            n = re.split('[,+]', hn)
            new_hn = ', '.join([x.upper() for x in n])
            new_hn = re.sub('/','-', new_hn)
            cleaned['20'][hn] = new_hn
        #e.g. b   => None
        elif re.compile('[0-9]+').search(hn) == None:
            cleaned['22'][hn] = None
        elif hn in entries_to_remove:
            cleaned['remove'][hn] = None
        elif hn in manual.keys():
            cleaned['manual'][hn] = manual[hn]
        else:
            cleaned['not edited'][hn] = hn
            unclean_hns.append(hn)
    db_ops.update_db(db, collection, 'address.housenumber', cleaned)

