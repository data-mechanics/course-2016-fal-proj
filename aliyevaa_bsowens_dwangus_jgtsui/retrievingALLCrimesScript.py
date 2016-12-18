import sys
import requests
import dml
import json
import time
import prov.model
import datetime
import uuid
import urllib.request

class crimes_all(dml.Algorithm):
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
    reads = []
    writes = ['aliyevaa_bsowens_dwangus_jgtsui.crimes_new']
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(crimes_all.contributor, crimes_all.contributor)
        
        link = 'https://data.cityofboston.gov/resource/7cdf-6fgx.json'
        limit = 50000
        offset = 0
        repo.dropPermanent("crimes_new")
        repo.createPermanent("crimes_new")
        
        x = 50000
        iterations = 0
        beginningTime = time.time()
        while x == 50000:
            response = urllib.request.urlopen(link + '?$limit=' + str(limit) + '&$offset=' + str(offset)).read().decode("utf-8")
            r = json.loads(response)
            repo['aliyevaa_bsowens_dwangus_jgtsui.crimes_new'].insert_many(r)
            offset += 50000
            x = len(r)
            iterations += 1
            print(iterations)
        print(repo['aliyevaa_bsowens_dwangus_jgtsui.crimes_new'].count())
        endRetrievalTime = time.time()
        #Processing that number of entries took: 55.37039828300476 seconds
        print("Processing that number of entries took: {} seconds".format(endRetrievalTime - beginningTime))
        
        countProcessed = 0
        numActual = 0
        for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.crimes_new.find( modifiers={"$snapshot": True} ):
            lng = elem['location']['longitude']
            lat = elem['location']['latitude']
            s = "0.0"
            if (s not in lng) and (s not in lat):
            #if (s not in lng) or (s not in lat):#Asselya nooooo
                repo.aliyevaa_bsowens_dwangus_jgtsui.crimes_new.update({'_id': elem['_id']}, {'$set': {'location': {'type': 'Point', 'coordinates': [float(lng),float(lat)]}}})
                numActual += 1
            else:
                repo.aliyevaa_bsowens_dwangus_jgtsui.crimes_new.remove(elem)

            countProcessed += 1
            if (countProcessed % 10000) == 0:
                print(countProcessed)
        
        #Entering 253075 number of entries (of overall total total) into database took: 111.88838171958923 seconds
        print("Entering {} number of entries (of overall total total) into database took: {} seconds".format(numActual, time.time() - endRetrievalTime))
        print("Number of entries inserted into Mongo: {}".format(repo.aliyevaa_bsowens_dwangus_jgtsui.crimes_new.count()))
        
        repo.aliyevaa_bsowens_dwangus_jgtsui.crimes_new.create_index([('location', '2dsphere')])
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client =  dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(crimes_all.contributor, crimes_all.contributor)
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')	
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#crimes_all',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_liquor_data, this_script)
        found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#crimes_new', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)	
        doc.wasGeneratedBy(found, get_liquor_data, endTime)
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()
        return doc

#crimes_all.execute()
#doc=crimes_all.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

def main():
    print("Executing: retrieveAllCrimesScript.py")
    crimes_all.execute()
    doc = crimes_all.provenance()

## eof
