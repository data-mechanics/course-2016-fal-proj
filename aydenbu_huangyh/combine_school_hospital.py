import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *



class SchoolandHospital(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.zip_hospitals_count','aydenbu_huangyh.zip_PublicSchool_count']
    writes = ['aydenbu_huangyh.school_hospital']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        schools = repo['aydenbu_huangyh.zip_PublicSchool_count']
        hospitals = repo['aydenbu_huangyh.zip_hospitals_count']

        hospitals_array = []
        for document in hospitals.find():
            hospitals_array.append({"zip":document['_id'], 'value':{"numofHospital": document['value']['numofHospital'],
                                                                    'numofSchool': 0}})

        schools_array = []
        for document in schools.find():
            schools_array.append({"zip":document['_id'], "value":{"numofSchool": document['value']['numofSchool'],
                                                                  'numofHospital': 0}})

        repo.dropPermanent("test")
        repo.createPermanent("test")
        repo['aydenbu_huangyh.test'].insert_many(schools_array)
        repo['aydenbu_huangyh.test'].insert_many(hospitals_array)

        test = repo['aydenbu_huangyh.test']

        # MapReduce function

        #def transform()




        map = Code("""
                    function(){
                           emit(this.zip,this.value);
                            }
                    """)

        reducer = Code("""
                function(key,values){

                        var result = {
                        "numofSchool" : '',
                        "numofHospital" : ''
                        };


                        values.forEach(function(value) {
                        if(value.numofHospital !== 0) {result.numofHospital = value.numofHospital;}
                        if(value.numofSchool !== 0) {result.numofSchool += value.numofSchool;}

                         });

                        return result;
                        }





                        """)




        repo.dropPermanent("school_hospital")

        res = test.map_reduce(map, reducer, 'aydenbu_huangyh.school_hospital')

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

        this_script = doc.agent('alg:aydenbu_huangyh#school_hospital',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:school_hospital_merge',
                              {'prov:label': 'school_hospital', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_school_hospital_count = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "Count the number of CornerStores in each zip"})
        doc.wasAssociatedWith(get_school_hospital_count, this_script)
        doc.usage(get_zip_PublicSchool_count, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        school_hospital_count = doc.entity('dat:aydenbu_huangyh#school_hospital',
                                         {prov.model.PROV_LABEL: 'Public Schools Count',
                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(school_hospital_count, this_script)
        doc.wasGeneratedBy(school_hospital_count,get_school_hospital_count, endTime)
        doc.wasDerivedFrom(school_hospital_count, resource, get_school_hospital_count, get_school_hospital_count,
                           get_school_hospital_count)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


SchoolandHospital.execute()
doc = SchoolandHospital.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## add provenance
## eof
