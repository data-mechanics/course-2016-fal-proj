import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *


class merge(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.zip_hospitals_count',
             'aydenbu_huangyh.zip_Healthycornerstores_count']
    writes = ['aydenbu_huangyh.merge_store_hospital']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        # Get the collections
        hospitals = repo['aydenbu_huangyh.zip_hospitals_count']
        stores = repo['aydenbu_huangyh.zip_Healthycornerstores_count']



        hospitals_array = []
        for document in hospitals.find():
            hospitals_array.append(
                {"zip": document['_id'], 'value': {"numofHospital": document['value']['numofHospital'],
                                                   'numofStore': 0}})
        stores_array = []
        for document in stores.find():
            stores_array.append({"zip": document['_id'], "value": {'numofHospital': 0,
                                                                    "numofStore": document['value']['numofStore']}})

        repo.dropPermanent("testforhospitalstores")
        repo.createPermanent("testforhospitalstores")
        repo['aydenbu_huangyh.testforhospitalstores'].insert_many(stores_array)
        repo['aydenbu_huangyh.testforhospitalstores'].insert_many(hospitals_array)

        test = repo['aydenbu_huangyh.testforhospitalstores']

        # MapReduce function

        # def transform()


        map = Code("""
                           function(){
                                  emit(this.zip,this.value);
                                   }
                           """)

        reducer = Code("""
                       function(key,values){

                               var result = {
                               "numofStore" : 0,
                               "numofHospital" : 0
                               };


                               values.forEach(function(value) {
                               if(value.numofHospital !== 0) {result.numofHospital += value.numofHospital;}
                               if(value.numofStore !== 0) {result.numofStore += value.numofStore;}

                                });

                               return result;
                               }


                               """)

        repo.dropPermanent("merge_store_hospital")
        repo.createPermanent("merge_store_hospital")

        res = test.map_reduce(map, reducer, 'aydenbu_huangyh.merge_store_hospital')



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

        '''
        '''
        # The agent
        this_script = doc.agent('alg:aydenbu_huangyh#merge_store_hospital',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # The source entity
        store_source = doc.entity('dat:healthy_corner_store_count',
                              {'prov:label': 'HealthyCorner Store Count', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        hospital_source = doc.entity('dat:hospital_count',
                                  {'prov:label': 'Hospital Count', prov.model.PROV_TYPE: 'ont:DataResource',
                                   'ont:Extension': 'json'})

        # The activity
        get_zip_health = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "Merge the numbers of hospital and HealthyStore in each zip"})

        # The activity is associated with the agent
        doc.wasAssociatedWith(get_zip_health, this_script)

        # Usage of the activity: Source Entity
        doc.usage(get_zip_health, store_source, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_zip_health, hospital_source, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        # The Result Entity
        zip_health = doc.entity('dat:aydenbu_huangyh#merge_store_hospital',
                                            {prov.model.PROV_LABEL: 'Merge Store and Hospital Count',
                                             prov.model.PROV_TYPE: 'ont:DataSet'})

        # Result Entity was attributed to the agent
        doc.wasAttributedTo(zip_health, this_script)

        # Result Entity was generated by the activity
        doc.wasGeneratedBy(zip_health, get_zip_health, endTime)

        # Result Entity was Derived From Source Entity
        doc.wasDerivedFrom(zip_health, store_source, get_zip_health, get_zip_health,
                           get_zip_health)
        doc.wasDerivedFrom(zip_health, hospital_source, get_zip_health, get_zip_health,
                           get_zip_health)

        repo.record(doc.serialize())
        repo.logout()

        return doc

merge.execute()
doc = merge.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))