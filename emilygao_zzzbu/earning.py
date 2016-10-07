import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class earning(dml.Algorithm):
    contributor = 'emilygao_zzzbu'
    reads = []
    writes = ['emilygao_zzzbu.earning']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Employee Earnings Report 2015 Data from Boston Gov .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        url = 'https://data.cityofboston.gov/resource/bejm-5s9g.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("earning")
        repo.createPermanent("earning")
        repo['emilygao_zzzbu.earning'].insert_many(r)

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

        this_script = doc.agent('alg:emilygao_zzzbu#earning', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'Employee Earnings Report 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_earning = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_earning, this_script)
        doc.usage(get_earning, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        earning = doc.entity('dat:emilygao_zzzbu#earning', {prov.model.PROV_LABEL:'Employee Earnings Report 2015', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(earning, this_script)
        doc.wasGeneratedBy(earning, get_earning, endTime)
        doc.wasDerivedFrom(earning, resource, get_earning, get_earning, get_earning)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

earning.execute()
doc = earning.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
