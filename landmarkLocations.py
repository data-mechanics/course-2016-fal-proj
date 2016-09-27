import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'ktan_ngurung'
    reads = []
    writes = ['ktan_ngurung.bigBelly', 'ktan_ngurung.colleges', 'ktan_ngurung.hubways', 'ktan_ngurung.busStops', 'ktan_ngurung.tStops']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')

        filename = 'json-data/big-belly-locations.json'
        res = open(filen, 'r')
        r0 = json.load(res)
        repo.dropPermanent("bigBelly")
        repo.createPermanent("bigBelly")
        repo['ktan_ngurung.bigBelly'].insert_many(r0)

        filename = 'json-data/colleges-and-universities.json'
        res = open(filen, 'r')
        r1 = json.load(res)
        repo.dropPermanent("colleges")
        repo.createPermanent("colleges")
        repo['ktan_ngurung.colleges'].insert_many(r1)

        filename = 'json-data/hubway-stations-in-boston.json'
        res = open(filen, 'r')
        r2 = json.load(res)
        repo.dropPermanent("hubways")
        repo.createPermanent("hubways")
        repo['ktan_ngurung.hubways'].insert_many(r2)

        filename = 'mbta-bus-stops.json'
        res = open(filen, 'r')
        r3 = json.load(res)
        repo.dropPermanent("busStops")
        repo.createPermanent("busStops")
        repo['ktan_ngurung.busStops'].insert_many(r3)

        filename = 't-stop-locations.json'
        res = open(filen, 'r')
        r4 = json.load(res)
        repo.dropPermanent("tStops")
        repo.createPermanent("tStops")
        repo['ktan_ngurung.tStops'].insert_many(r4)

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
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ktan_ngurung#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
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

        lost = doc.entity('dat:ktan_ngurung#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:ktan_ngurung#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
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