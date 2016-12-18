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


class entertainment_full_retrieval(dml.Algorithm):
    # Set up the database connection.
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
    reads = []
    writes = ['aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(entertainment_full_retrieval.contributor, entertainment_full_retrieval.contributor)
        
        link = 'https://data.cityofboston.gov/resource/cz6t-w69j.json'
        limit = 50000
        offset = 0
        repo.dropPermanent("entertainment_licenses")
        repo.createPermanent("entertainment_licenses")
        
        print("Num entries after dropping table: {}".format(repo['aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses'].count()))
        
        x = 50000
        while x == 50000:
            response = urllib.request.urlopen(link + '?$limit=' + str(limit) + '&$offset=' + str(offset)).read().decode("utf-8")
            r = json.loads(response)
            repo['aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses'].insert_many(r)
            offset += 50000
            x = len(r)
        print("Retrieved {} number of entertainment licenses.".format(repo['aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses'].count()))
        
        print("Transforming entertainment_licenses dataset...")
        
        numLicenses = 0
        for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.find():
            if elem['location'] != "NULL" and type(elem['location']) == str and elem['location'].startswith('('):
                #repo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.update({'_id': elem['_id']}, {'$set': {
                #    'location': {'type': 'Point', 'coordinates': [float(elem['location'][1:12]), float(elem['location'][15:-1])]}}})
                #numLicenses += 1

                #in combineRestaurant.py
                #doc['address']
                #doc['location']['coordinates']
                #doc['dbaname']
                #doc['_id']
                #in cleanup.py
                #["licensedttm"]
                #["licenseno"]

                if 'licensedttm' in elem.keys():
                    lttm = elem['licensedttm']
                else:
                    lttm = None
                if "licenseno" in elem.keys():
                    licenseNo = elem["licenseno"]
                else:
                    licenseNo = None
                if 'dbaname' in elem.keys():
                    dba = elem['dbaname']
                else:
                    dba = None
                if 'address' in elem.keys():
                    addr = elem['address']
                else:
                    addr = None
                
                prevCoords = ast.literal_eval(elem['location'])
                if prevCoords[1] < 0 and prevCoords[0] > 0:
                    repo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.update({'_id': elem['_id']}, {'$set': {
                            'location': {'type': 'Point', 'coordinates': [prevCoords[1], prevCoords[0]]}, 'address': addr, 'dbaname': dba, 'licensedttm': lttm, "licenseno": licenseNo}})
                    numLicenses += 1
            else:
                repo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.remove(elem)
        try:
            repo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.create_index([('location', '2dsphere')])
        except:
            pass
        #repo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.create_index([('location', '2dsphere')])

        print("Number of ACTUAL Entertainment Licenses Retrieved: {}".format(numLicenses))
        print("Num entries after inserting into table: {}".format(repo['aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses'].count()))
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(entertainment_full_retrieval.contributor, entertainment_full_retrieval.contributor)
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#entertainment_full_retrieval',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        get_liquor_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_liquor_data, this_script)
        found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#entertainment_licenses',
                           {prov.model.PROV_LABEL: '', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_liquor_data, endTime)
        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()
        return doc


#entertainment_full_retrieval.execute()
#doc = entertainment_full_retrieval.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

def main():
    print("Executing: entertainment_full_retrieval.py")
    entertainment_full_retrieval.execute()
    doc = entertainment_full_retrieval.provenance()
