
# coding: utf-8

# In[13]:

import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
import pprint


contributor = 'aditid_benli95_teayoon_tyao'
reads = ['aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius', 'aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug']
writes = ['aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment', 'aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment']


client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')



##reset resulting directory
repo.dropPermanent('aditid_benli95_teayoon_tyao.listOfAverages')
repo.createPermanent('aditid_benli95_teayoon_tyao.listOfAverages')

"""
Creates JSON file of all Histogram Distributions. Do not run this as the file has already been 
created and save in data.json
"""

jsonDict = {}

for d in range(0,50):             #to account for floats
    rad = radius + (d/10)
    
    add_on = (radius*10)+d
    
    jsonDict[add_on] = {}
    jsonDict[add_on]["all"] = {}
    jsonDict[add_on]["drug"] = {}

    
    repo_string_all = "repo_all = repo.aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment" + str(add_on) + ".find()"
    exec(repo_string_all)
    
    for a in repo_all:
        crimeDict = dict(a)
        
        jsonDict[add_on]["all"][crimeDict["_id"]] = crimeDict["value"]["crimes"]
        
    repo_string_drug = "repo_drug = repo.aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment" + str(add_on) + ".find()"
    exec(repo_string_drug)
    
    for a in repo_drug:
        crimeDict = dict(a)
        
        jsonDict[add_on]["drug"][crimeDict["_id"]] = crimeDict["value"]["crimes"]


with open('data2.json', 'w') as f:   #changed file name to data2 to prevent accidental overwriting
     json.dump(jsonDict, f)
        
        

