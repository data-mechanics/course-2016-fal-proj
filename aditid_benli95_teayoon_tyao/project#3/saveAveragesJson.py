
# coding: utf-8

# In[10]:

import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code


contributor = 'aditid_benli95_teayoon_tyao'
reads = ['aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius', 'aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug']
writes = ['aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment', 'aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment']


client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')



##reset resulting directory
repo.dropPermanent('aditid_benli95_teayoon_tyao.listOfAverages')
repo.createPermanent('aditid_benli95_teayoon_tyao.listOfAverages')

"""Change this Radius accordingly"""
radius = 3

diffJson = {}

for d in range(0,10):             #to account for floats
    rad = radius + (d/10)
    
    add_on = (radius*10)+d

    
    repo_string_all = "repo_all = repo.aditid_benli95_teayoon_tyao.averageAll" + str(add_on) + ".find()"
    exec(repo_string_all)
    
    for a in repo_all:
        avgDict = dict(a)
        
        numer = avgDict["value"]["product"]
        denom = avgDict["value"]["crimes"]
        avg_all = numer / denom
        #print("avg_all: " + str(avg_all) )
    
    repo_string_drug = "repo_drug = repo.aditid_benli95_teayoon_tyao.averageDrug" + str(add_on) + ".find()"
    exec(repo_string_drug)
    
    for a in repo_drug:
        avgDict = dict(a)
        
        numer = avgDict["value"]["product"]
        denom = avgDict["value"]["crimes"]
        avg_drug = numer / denom
        #print("avg_drug: " + str(avg_drug) )


    thisEntry = {"avg_all": avg_all, "avg_drug": avg_drug, "difference": avg_all - avg_drug, "distance": rad}
    #print("diff: " + str(avg_all - avg_drug) )

    res = repo.aditid_benli95_teayoon_tyao.listOfAverages.insert_one(thisEntry)
    diffJson[rad] = {}
    diffJson[rad]["avg_all"] = avg_all
    diffJson[rad]["avg_drug"] = avg_drug
    diffJson[rad]["diff"] = avg_all - avg_drug


with open('average.json', 'w') as f: 
    json.dump(diffJson, f)





