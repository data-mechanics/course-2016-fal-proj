import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


def get_Col(db,repo):
    '''input: string representing database name db
       output: returns list with all key,values from database db
    '''
    col_list = []
    print("Getting collection", db + "....")
    for elem in repo[db].find({}):
        print(elem)
        col_list.append(elem)
    return col_list


client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ckarjadi_johnnyg7','ckarjadi_johnnyg7')

collections = repo.collection_names()
d = {}
for col in collections:
    d[col] = get_Col(col,repo)

    
repo.logout()

for x in d:
    filename = x + '_OUTPUT.json'
    collection = d[x]
    out = open(filename,'w')
    check= len(collection)
    for x in range(len(collection)):
        if x!=check-1:
            










        
