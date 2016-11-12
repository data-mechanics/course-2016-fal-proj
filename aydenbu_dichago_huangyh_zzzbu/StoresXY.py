import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *



class countHealthyCornerStores(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.healthyCornerStores']
    writes = ['aydenbu_huangyh.zip_Healthycornerstores_XY']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection
        # client = dml.pymongo.MongoClient()
        # repo = client.repo
        # repo.authenticate('aydenbu_huangyh', 'aydenbu_huangyh')

        # Connect to the Database
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        stores = repo['aydenbu_huangyh.healthyCornerStores']

        count_stores_zip_XY = []
        for document in stores.find():
            if document is not None:
                record = {'_id': document['_id'],
                          'zipCode':  '0' +document['zip'],
                          'location': [document['location']['coordinates'][1],
                                       document['location']['coordinates'][0]]
                          }
                count_stores_zip_XY.append(record)


        repo.dropPermanent('zip_Healthycornerstores_XY')
        repo.createPermanent('zip_Healthycornerstores_XY')
        repo['aydenbu_huangyh.zip_Healthycornerstores_XY'].insert_many(count_stores_zip_XY)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aydenbu_huangyh#countHealthyCornerStores',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:healthyCornerStores',
                              {'prov:label': 'HealthyCornerStore XY', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_zip_Healthycornerstores_XY = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "get the XY coordinates of each stores"})
        doc.wasAssociatedWith(get_zip_Healthycornerstores_XY, this_script)
        doc.usage(get_zip_Healthycornerstores_XY, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        zip_Healthycornerstores_XY = doc.entity('dat:aydenbu_huangyh#zip_Healthycornerstores_XY',
                                         {prov.model.PROV_LABEL: 'Healthy Corner Stores XY',
                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(zip_Healthycornerstores_XY, this_script)
        doc.wasGeneratedBy(zip_Healthycornerstores_XY, get_zip_Healthycornerstores_XY, endTime)
        doc.wasDerivedFrom(zip_Healthycornerstores_XY, resource, get_zip_Healthycornerstores_XY, get_zip_Healthycornerstores_XY,
                           get_zip_Healthycornerstores_XY)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


countHealthyCornerStores.execute()
doc = countHealthyCornerStores.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## add provenance
## eof
