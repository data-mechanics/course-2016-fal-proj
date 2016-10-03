import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class trash(dml.Algorithm):
    contributor = 'emilygao_zzzbu'
    reads = []
    writes = ['emilygao_zzzbu.address']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Trash Schedules by Address data from Boston Gov .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emilygao_zzzbu', 'emilygao_zzzbu')

        url = 'https://data.cityofboston.gov/resource/cp2t-tvxx.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("trashScheduals")
        repo.createPermanent("trashScheduals")
        repo['emilygao_zzzbu.trashScheduals'].insert_many(r)

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
        repo.authenticate('emilygao_zzzbu', 'emilygao_zzzbu')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:emilygao_zzzbu#trashScheduals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'Trash Scheduals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_scheduals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_scheduals, this_script)
        doc.usage(get_scheduals, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        scheduals = doc.entity('dat:emilygao_zzzbu#trashScheduals', {prov.model.PROV_LABEL:'Trash Scheduals', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(scheduals, this_script)
        doc.wasGeneratedBy(scheduals, get_scheduals, endTime)
        doc.wasDerivedFrom(scheduals, resource, get_scheduals, get_scheduals, get_scheduals)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

trash.execute()
doc = trash.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
