import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
#import prepData1


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

for d in range(80, 105, 5):
    d = d / 10

#print("value of d: " + str(d))
# prepData1.prepData1.execute(d)

    print("starting prepData2 now")
    import prepData2

#    prepData2.prepData2.execute()

    repo_all = repo.aditid_benli95_teayoon_tyao.averageAll.find()
    for a in repo_all:
        avgDict = dict(a)
        
        numer = avgDict["value"]["product"]
        denom = avgDict["value"]["crimes"]
        avg_all = numer / denom
        print("avg_all: " + str(avg_all) )
    

    repo_drug = repo.aditid_benli95_teayoon_tyao.averageDrug.find()
    for a in repo_drug:
        avgDict = dict(a)
        
        numer = avgDict["value"]["product"]
        denom = avgDict["value"]["crimes"]
        avg_drug = numer / denom
        print("avg_drug: " + str(avg_drug) )


    thisEntry = {"avg_all": avg_all, "avg_drug": avg_drug, "difference": avg_all - avg_drug, "distance": d}


    res = repo.aditid_benli95_teayoon_tyao.listOfAverages.insert_one(thisEntry)
    arr_of_diff.append(avg_all - avg_drug)

print(arr_of_diff)



#find max









