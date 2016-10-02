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
    reads = ['aydenbu_huangyh.merge_school_garden',
             'aydenbu_huangyh.merge_store_hospital']
    writes = ['aydenbu_huangyh.zip_public']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        # Get the collections
        school_garden = repo['aydenbu_huangyh.merge_school_garden']
        store_hospital = repo['aydenbu_huangyh.merge_store_hospital']


        # For every document in hospitals zip find the number of store that associate with that zip
        zip_public = []
        for document in school_garden.find():
            store_hospital_count = store_hospital.find_one({'_id': document['_id']}, {'_id': False, 'value.numofStore': True,
                                                                               'value.numofHospital': True})
            if store_hospital_count is None:
                zip = {'_id': document['_id'],
                        'value': {
                            'numofGarden': document['value']['numofGarden'],
                            'numofSchool': document['value']['numofSchool'],
                            'numofHospital': 0.0,
                            'numofStore': 0.0}  # Assign the 0 to the num if there is no result
                       }
                zip_public.append(zip)
                continue
            else:
                zip = {'_id': document['_id'],
                        'value': {
                            'numofGarden': document['value']['numofGarden'],
                            'numofSchool': document['value']['numofSchool'],
                            'numofHospital': store_hospital_count['value']['numofHospital'],
                            'numofStore': store_hospital_count['value']['numofStore']}
                       }
                zip_public.append(zip)
        ''''''''''''''''''''''''''''''''''''''''''''''''


        # Create a new collection and insert the result data set
        repo.dropPermanent("zip_public")
        repo.createPermanent("zip_public")
        repo['aydenbu_huangyh.zip_public'].insert_many(zip_public)

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

        '''
        '''
        # The agent
        this_script = doc.agent('alg:aydenbu_huangyh#merge_all_public_buildings',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # The source entity
        school_garden_source = doc.entity('dat:merge_school_garden',
                              {'prov:label': 'Public School And Garden Count', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        store_hospital_source = doc.entity('dat:merge_store_hospital',
                                  {'prov:label': 'Store and Hospital Count', prov.model.PROV_TYPE: 'ont:DataResource',
                                   'ont:Extension': 'json'})

        # The activity
        get_zip_public = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "Merge the numbers of all public buildings based on zipcode"})

        # The activity is associated with the agent
        doc.wasAssociatedWith(get_zip_public, this_script)

        # Usage of the activity: Source Entity
        doc.usage(get_zip_public, school_garden_source, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_zip_public, store_hospital_source, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        # The Result Entity
        zip_public = doc.entity('dat:aydenbu_huangyh#zip_public',
                                            {prov.model.PROV_LABEL: 'Merge All Public Building Count',
                                             prov.model.PROV_TYPE: 'ont:DataSet'})

        # Result Entity was attributed to the agent
        doc.wasAttributedTo(zip_public, this_script)

        # Result Entity was generated by the activity
        doc.wasGeneratedBy(zip_public, get_zip_public, endTime)

        # Result Entity was Derived From Source Entity
        doc.wasDerivedFrom(zip_public, school_garden_source, get_zip_public, get_zip_public,
                           get_zip_public)
        doc.wasDerivedFrom(zip_public, store_hospital_source, get_zip_public, get_zip_public,
                           get_zip_public)

        repo.record(doc.serialize())
        repo.logout()

        return doc

merge.execute()
doc = merge.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))