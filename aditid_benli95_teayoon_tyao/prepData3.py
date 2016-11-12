import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
import prepData1
import prepData2


contributor = 'aditid_benli95_teayoon_tyao'
reads = ['aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius', 'aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug']
writes = ['aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment', 'aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment']


client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')



#reset resulting directory
repo.dropPermanent('aditid_benli95_teayoon_tyao.listOfAverages')
repo.createPermanent('aditid_benli95_teayoon_tyao.listOfAverages')

arr_of_diff = []

for d in range(1,3):
    #prepData1.execute(d)

    #prepData2.execute()

    repo = repo.aditid_benli95_teayoon_tyao.averageAll.find()
    for a in repo:
        avgDict = dict(a)

        numer = avgDict["value"]["crimes"]
        denom = avgDict["value"]["product"]
        avg_all = numer / denom
    

    repo = repo.aditid_benli95_teayoon_tyao.averageDrugs.find()
    for a in repo:
        avgDict = dict(a)
        
        numer = avgDict["value"]["crimes"]
        denom = avgDict["value"]["product"]
        avg_drug = numer / denom


    thisEntry = {"avg_all": avg_all, "avg_drug": avg_drug, "difference": avg_all - avg_drug, "distance": d}


    res = repo.aditid_benli95_teayoon_tyao.listOfAverages.insert_one(thisEntry)
    arr_of_diff.append(avg_all - avg_drug)



#find max
#either through above array
#or through the dictionary












