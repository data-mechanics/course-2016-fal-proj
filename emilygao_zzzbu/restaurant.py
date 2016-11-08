import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class restaurant(dml.Algorithm):
    contributor = 'emilygao_zzzbu'
    reads = []
    writes = ['emilygao_zzzbu.restaurant']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Active Food Establishment Licenses Data from Boston Gov .'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        url = 'https://data.cityofboston.gov/resource/fdxy-gydq.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("restaurant")
        repo.createPermanent("restaurant")
        repo['emilygao_zzzbu.restaurant'].insert_many(r)

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

        this_script = doc.agent('alg:emilygao_zzzbu#restaurant', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'Active Food Establishment Licenses', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_restaurant = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_restaurant, this_script)
        doc.usage(get_restaurant, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        restaurant = doc.entity('dat:emilygao_zzzbu#restaurant', {prov.model.PROV_LABEL:'Active Food Establishment Licenses', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(restaurant, this_script)
        doc.wasGeneratedBy(restaurant, get_restaurant, endTime)
        doc.wasDerivedFrom(restaurant, resource, get_restaurant, get_restaurant, get_restaurant)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

restaurant.execute()
doc = restaurant.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
