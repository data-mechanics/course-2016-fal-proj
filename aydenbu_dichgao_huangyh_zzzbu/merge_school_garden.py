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
    reads = ['aydenbu_huangyh.zip_PublicSchool_count',
             'aydenbu_huangyh.zip_communityGardens_count']
    writes = ['aydenbu_huangyh.zip_public']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        # Get the collections
        gardens = repo['aydenbu_huangyh.zip_communityGardens_count']
        schools = repo['aydenbu_huangyh.zip_PublicSchool_count']

        schools_array = []
        for document in schools.find():
            schools_array.append(
                {"zip": document['_id'], 'value': {"numofSchool": document['value']['numofSchool'],
                                                   'numofGarden': 0}})
        gardens_array = []
        for document in gardens.find():
            gardens_array.append({"zip": document['_id'], "value": {'numofSchool': 0,
                                                                   "numofGarden": document['value']['numofGarden']}})

        repo.dropPermanent("testforschoolgarden")
        repo.createPermanent("testforschoolgarden")
        repo['aydenbu_huangyh.testforschoolgarden'].insert_many(schools_array)
        repo['aydenbu_huangyh.testforschoolgarden'].insert_many(gardens_array)

        test = repo['aydenbu_huangyh.testforschoolgarden']

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
                                       "numofSchool" : 0,
                                       "numofGarden" : 0
                                       };


                                       values.forEach(function(value) {
                                       if(value.numofSchool !== 0) {result.numofSchool += value.numofSchool;}
                                       if(value.numofGarden !== 0) {result.numofGarden += value.numofGarden;}

                                        });

                                       return result;
                                       }


                                       """)

        repo.dropPermanent("merge_school_garden")
        repo.createPermanent("merge_school_garden")

        res = test.map_reduce(map, reducer, 'aydenbu_huangyh.merge_school_garden')


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
        this_script = doc.agent('alg:aydenbu_huangyh#merge_school_garden',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # The source entity
        school_source = doc.entity('dat:public_school_count',
                              {'prov:label': 'Public School Count', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        garden_source = doc.entity('dat:community_garden_count',
                                  {'prov:label': 'Community Garden Count', prov.model.PROV_TYPE: 'ont:DataResource',
                                   'ont:Extension': 'json'})

        # The activity
        get_zip_public = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "Merge the numbers of garden and school in each zip"})

        # The activity is associated with the agent
        doc.wasAssociatedWith(get_zip_public, this_script)

        # Usage of the activity: Source Entity
        doc.usage(get_zip_public, school_source, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_zip_public, garden_source, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        # The Result Entity
        zip_public = doc.entity('dat:aydenbu_huangyh#merge_school_garden',
                                            {prov.model.PROV_LABEL: 'Merge Public Building Count',
                                             prov.model.PROV_TYPE: 'ont:DataSet'})

        # Result Entity was attributed to the agent
        doc.wasAttributedTo(zip_public, this_script)

        # Result Entity was generated by the activity
        doc.wasGeneratedBy(zip_public, get_zip_public, endTime)

        # Result Entity was Derived From Source Entity
        doc.wasDerivedFrom(zip_public, school_source, get_zip_public, get_zip_public,
                           get_zip_public)
        doc.wasDerivedFrom(zip_public, garden_source, get_zip_public, get_zip_public,
                           get_zip_public)

        repo.record(doc.serialize())
        repo.logout()

        return doc

merge.execute()
doc = merge.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))