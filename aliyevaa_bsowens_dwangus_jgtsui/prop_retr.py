import sys
import requests
import dml
import json
import time
import prov.model
import datetime
import uuid
import ast
import urllib.request

class prop_retr(dml.Algorithm):
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
    reads = []
    writes = ['aliyevaa_bsowens_dwangus_jgtsui.prop_retr']
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(prop_retr.contributor, prop_retr.contributor)
        
        link= 'https://data.cityofboston.gov/resource/i7w8-ure5.json'
        limit = 50000
        offset = 0
        repo.dropPermanent("property_assessment")
        repo.createPermanent("property_assessment")
        
        x = 50000
        iterations = 0
        beginningTime = time.time()
        while x == 50000:
            response = urllib.request.urlopen(link + '?$limit=' + str(limit) + '&$offset=' + str(offset)).read().decode("utf-8")
            r=json.loads(response)
            repo['aliyevaa_bsowens_dwangus_jgtsui.property_assessment'].insert_many(r)
            offset += 50000
            x = len(r)
            iterations += 1
            print(iterations)

        #169199
        print(repo['aliyevaa_bsowens_dwangus_jgtsui.property_assessment'].count())
        endRetrievalTime = time.time()
        #Processing that number of entries took: 40.506370306015015 seconds
        print("Processing that number of entries took: {} seconds".format(endRetrievalTime - beginningTime))
        print("Transforming property assessment dataset...")

        countProcessed = 0
        numActual = 0
        for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.property_assessment.find():
            if elem['latitude']!="0" and elem['longitude']!="0" and elem['latitude']!="#N/A" and elem['longitude']!="#N/A":
                repo.aliyevaa_bsowens_dwangus_jgtsui.property_assessment.update({'_id': elem['_id']}, {'$set': {'location': {'type': 'Point', 'coordinates': [float(elem['longitude']),float(elem['latitude'])]}}})
                numActual += 1
            else:
                repo.aliyevaa_bsowens_dwangus_jgtsui.property_assessment.remove(elem)

            countProcessed += 1
            if (countProcessed % 10000) == 0:
                print(countProcessed)
                
        repo.aliyevaa_bsowens_dwangus_jgtsui.property_assessment.create_index([('location', '2dsphere')])
        #Entering 101842 number of entries (of overall total total) into database took: 84.45189452171326 seconds
        print("Entering {} number of entries (of overall total total) into database took: {} seconds".format(numActual, time.time() - endRetrievalTime))
        print("Number of entries inserted into Mongo: {}".format(repo.aliyevaa_bsowens_dwangus_jgtsui.property_assessment.count()))

        repo.aliyevaa_bsowens_dwangus_jgtsui.crimes_new.create_index([('location', '2dsphere')])
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client =  dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(prop_retr.contributor, prop_retr.contributor)
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#prop_retr',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_liquor_data, this_script)
        found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#crimes_new', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_liquor_data, endTime)
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()
        return doc
    
#prop_retr.execute()
#doc = prop_retr.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

def main():
    print("Executing: prop_retr.py")
    prop_retr.execute()
    doc = prop_retr.provenance()
