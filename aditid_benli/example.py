import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty

class example(dml.Algorithm):
    contributor = 'aditid_benli'
    reads = []
    writes = ['aditid_benli.jam', 'aditid_benli.comparking', 'aditid_benli.inters', 'aditid_benli.metparking', 'aditid_benli.partickets']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli', 'aditid_benli')
        
        url = 'https://data.cityofboston.gov/resource/yqgx-2ktq.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("jam")
        repo.createPermanent("jam")
        repo['aditid_benli.jam'].insert_many(r)

        
        url = 'https://data.cambridgema.gov/api/views/impv-6fac/rows.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("inters")
        repo.createPermanent("inters")
        repo['aditid_benli.inters'].insert_one(r)
        
        url = 'https://data.cambridgema.gov/api/views/up94-ihbw/rows.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("metparking")
        repo.createPermanent("metparking")
        repo['aditid_benli.metparking'].insert_one(r)
        
        url = 'https://data.cambridgema.gov/api/views/vnxa-cuyr/rows.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        data = r['data']
        newData = []                                                                #tranformation #1
        for i in range(len(data)):
            if data[i][-2] == "NO PARKING" or data[i][-2] == "RESIDENT PERMIT ONLY":   
                newData.append(data[i])                                                     #Filters out ticketing data that aren't relevant to parking demand in an area, i.e double parking or meter expiration
        r['data'] = newData
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("partickets")
        repo.createPermanent("partickets")
        repo['aditid_benli.partickets'].insert_many(r)
        
        url = 'https://data.cambridgema.gov/api/views/vr3p-e9ke/rows.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("comparking")
        repo.createPermanent("comparking")
        repo['aditid_benli.comparking'].insert_one(r)


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}


class example(dml.Algorithm):
    contributor = 'aditid_benli'
    reads = []
    writes = ['aditid_benli.jam', 'aditid_benli.comparking', 'aditid_benli.inters', 'aditid_benli.metparking', 'aditid_benli.partickets']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli', 'aditid_benli')
        
        


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alice_bob', 'alice_bob')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                }
            )
        doc.usage(get_lost, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                }
            )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof