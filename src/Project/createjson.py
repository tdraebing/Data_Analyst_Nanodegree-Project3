#####################################################################
#Script to generate a JSON document from the OpenStreetMap XML file.#
#####################################################################

import xml.etree.cElementTree as ET
import re
import codecs
import json

def create_entry(element):
    #This function creates the dictionary for each node
    entry = {}
    #First add all mandatory values (id, creation etc.)
    entry['id'] = element.get('id')
    entry['type'] = element.tag
    entry['visible'] = element.get('visible')
    entry['created'] = {}
    entry['created']['version'] = element.get('version')
    entry['created']['changeset'] = element.get('changeset')
    entry['created']['timestamp'] = element.get('timestamp')
    entry['created']['user'] = element.get('user')
    entry['created']['uid'] = element.get('uid')
	#Check whether the longitude and latitude are given, convert them to float and put them in a list
    if element.get('lat') != None and element.get('lon')!= None:
        entry['pos'] = [float(element.get('lat')),float(element.get('lon'))]
	#If the node type is way, the node_refs key is added and the ref-attributes of all nd elements are added
    if entry['type'] == 'way':
        entry['node_refs'] = []
        for tag in element.iter("nd"):
            entry['node_refs'].append(tag.get('ref'))
	#Looping through tag tags and filtering them
    for tag in element.iter("tag"):
        if tag.get('k') != None:
            #skip keys containing with problem chars
            problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
            if problemchars.search(tag.get('k')) != None:
                print('Key with problem chars: ' + tag.get('k'))
                continue
    		#skip values with more than one ':'
            elif len(re.findall('[:]',tag.get('k'))) > 1:
                continue
    		#add values with the addr: code
            elif re.findall('^[addr:]',tag.get('k')) != []:
    			#check whether the address key already exists, if not add it to dictionary
                if not 'address' in entry.keys():
                    entry['address'] = {}
    			#remove the addr:
                k = re.sub('addr:','',tag.get('k'))
                entry['address'][k] = tag.get('v')
                
            else:
    			#add all other tags to the dictionary
                entry[tag.get('k')] = tag.get('v')
    return entry

#function to loop through all parent tags of the XML file
def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" or element.tag == "relation":
        node = create_entry(element)
        return node
    else:
        return None

#function to parse the XML file and to write the JSON-file
def process_map(file_in):
    file_out = "{0}.json".format(file_in)
   
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in, events=("start", "end")):
            el = shape_element(element)
            element.clear()
            if el:
                # The following statement is needed since the xml contains mixed ways of ending tags
                if el['created']['uid'] == None:
                    continue
                # write the JSON file and encode the strings into utf-8
                fo.write(json.dumps(el, ensure_ascii=False).encode("utf-8")+"\n")
                #Clear element to save memory
                el.clear()
            