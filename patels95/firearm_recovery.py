import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class firearm_recovery(dml.Algorithm):
    contributor = 'patels95'
    reads = []
    writes = ['patels95.firearm_recovery']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('patels95', 'patels95')

        with open('../auth.json') as jsonFile:
            auth = json.load(jsonFile)

        socrataAppToken = auth["socrata"]["app"]

        # Boston Police Department Firearms Recovery Counts
        url = 'https://data.cityofboston.gov/resource/ffz3-2uqv.json?$$app_token=' + socrataAppToken
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        for i in range(len(r)):
            total = int(r[i]['crimegunsrecovered']) + int(r[i]['gunssurrenderedsafeguarded']) + \
             int(r[i]['buybackgunsrecovered'])
            r[i]['totalgunsrecovered'] = total
            r[i]['collectiondate'] = r[i]['collectiondate'][:10]

        repo.dropPermanent("firearm_recovery")
        repo.createPermanent("firearm_recovery")
        repo['patels95.firearm_recovery'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
       client = dml.pymongo.MongoClient()
       repo = client.repo
       repo.authenticate('patels95', 'patels95')

       doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
       doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
       doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
       doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
       doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

       this_script = doc.agent('alg:patels95#firearm_recovery', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
       resource = doc.entity('bdp:ffz3-2uqv', {'prov:label':'BPD Firearm Recovery', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
       this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
       doc.wasAssociatedWith(this_run, this_script)
       doc.usage(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

       firearm_recovery_counts = doc.entity('dat:patels95#firearm_recovery', {prov.model.PROV_LABEL:'Firearm Recovery', prov.model.PROV_TYPE:'ont:DataSet'})
       doc.wasAttributedTo(firearm_recovery_counts, this_script)
       doc.wasGeneratedBy(firearm_recovery_counts, this_run, endTime)
       doc.wasDerivedFrom(firearm_recovery_counts, resource, this_run, this_run, this_run)

       repo.record(doc.serialize())
       repo.logout()

       return doc

firearm_recovery.execute()
doc = firearm_recovery.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
