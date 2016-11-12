import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class chargingstations(dml.Algorithm):
    contributor = "asanentz_ldebeasi_mshop_sinichol"
    reads = []
    writes = ["asanentz_ldebeasi_mshop_sinichol.chargingstations"]

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asanentz_ldebeasi_mshop_sinichol', 'asanentz_ldebeasi_mshop_sinichol')

        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/465e00f9632145a1ad645a27d27069b4_2.geojson' 
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("")
        repo.createPermanent("chargingstations")
        repo['asanentz_ldebeasi_mshop_sinichol.chargingstations'].insert_many(r["features"])

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
        repo.authenticate('asanentz_ldebeasi_mshop_sinichol', 'asanentz_ldebeasi_mshop_sinichol')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/')

        this_script = doc.agent('alg:asanentz_ldebeasi_mshop_sinichol#chargingstations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bod:465e00f9632145a1ad645a27d27069b4_2', {'prov:label':'List of Charging Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )
        doc.usage(get_lost, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        lost = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#chargingstations', {prov.model.PROV_LABEL:'List of Charging Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

chargingstations.execute()
doc = chargingstations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))