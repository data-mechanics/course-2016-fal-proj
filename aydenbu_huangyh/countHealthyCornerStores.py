import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps



class countHealthyCornerStores(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.healthyCornerStores']
    writes = ['aydenbu_huangyh.zip_Healthycornerstores_count']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aydenbu_huangyh', 'aydenbu_huangyh')
        stores = repo['aydenbu_huangyh.healthyCornerStores']

        # MapReduce function
        mapper = Code("""
                        function() {
                            emit(this.zip, 1);
                        }
                      """)
        reducer = Code("""
                        function(k, vs) {
                            var total = 0;
                            for (var i = 0; i < vs.length; i++) {
                                total += vs[i];
                            }
                            return total;
                        }"""
                      )
        repo.dropPermanent("zip_Healthycornerstores_count")
        result = stores.map_reduce(mapper, reducer, "aydenbu_huangyh.zip_Healthycornerstores_count")

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aydenbu_huangyh', 'aydenbu_huangyh')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aydenbu_huangyh#countHealthyCornerStores',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:hospital_location',
                              {'prov:label': 'HealthyCornerStore Location', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_zip_Healthycornerstores_count = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "Count the number of CornerStores in each zip"})
        doc.wasAssociatedWith(get_zip_Healthycornerstores_count, this_script)
        doc.usage(get_zip_Healthycornerstores_count, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        zip_Healthycornerstores_count = doc.entity('dat:aydenbu_huangyh#zip_Healthycornerstores_count',
                                         {prov.model.PROV_LABEL: 'Healthy Corner Stores Count',
                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(zip_Healthycornerstores_count, this_script)
        doc.wasGeneratedBy(zip_Healthycornerstores_count, get_zip_Healthycornerstores_count, endTime)
        doc.wasDerivedFrom(zip_Healthycornerstores_count, resource, get_zip_Healthycornerstores_count, get_zip_Healthycornerstores_count,
                           get_zip_Healthycornerstores_count)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


countHealthyCornerStores.execute()
doc = countHealthyCornerStores.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## add provenance
## eof
