import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *


#  Count the hospital
class countHostipals(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.hospitalLocation']
    writes = ['aydenbu_huangyh.zip_hospitals_count_XY']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection
        # client = dml.pymongo.MongoClient()
        # repo = client.repo
        # repo.authenticate('aydenbu_huangyh', 'aydenbu_huangyh')

        # Connect to the Database
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        hospitals = repo['aydenbu_huangyh.hospitalLocation']

        # MapReduce function
        mapper = Code("""
                       function() {
                            emit(this.zipcode, {lat: this.location.coordinates[1], long: this.location.coordinates[0]});
                        }
                      """)

        reducer = Code("""
                        function(k, vs) {
                        }
                        """)
        count_hospital_zip_XY = []
        for document in hospitals.find():
            if document is not None:
                record = {'_id': document['_id'],
                          'zipCode': document['location_zip'],
                          'location': [document['location']['coordinates'][0],
                                       document['location']['coordinates'][1]]
                          }
                count_hospital_zip_XY.append(record)




        repo.dropPermanent("zip_hospitals_count_XY")
        repo.createPermanent("zip_hospitals_count_XY")
        repo['aydenbu_huangyh.zip_hospitals_count_XY'].insert_many(count_hospital_zip_XY)
        # repo.createPermanent("zip_hospitals_count")
        #result = hospitals.map_reduce(mapper, reducer, "aydenbu_huangyh.zip_hospitals_count_XY")

        '''
        # Save the result to the db
        # repo.dropPermanent("zip_hospitals_count")
        #repo.createPermanent("zip_hospitals_count")
        '''
        #repo['aydenbu_huangyh.zip_hospitals_count'].insert_many(result)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}



    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection.
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aydenbu_huangyh#countHospitals',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:hospital_location',
                              {'prov:label': 'Hospital Location', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_zip_hospitals_count = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_LABEL: "Count the number of hospital in each zip"})
        doc.wasAssociatedWith(get_zip_hospitals_count, this_script)
        doc.usage(get_zip_hospitals_count, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        zip_hospitals_count = doc.entity('dat:aydenbu_huangyh#zip_hospitals_count',
                          {prov.model.PROV_LABEL: 'Hospitals Count', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(zip_hospitals_count, this_script)
        doc.wasGeneratedBy(zip_hospitals_count, get_zip_hospitals_count, endTime)
        doc.wasDerivedFrom(zip_hospitals_count, resource, get_zip_hospitals_count, get_zip_hospitals_count, get_zip_hospitals_count)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc



countHostipals.execute()
doc = countHostipals.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## add provenance
## eof