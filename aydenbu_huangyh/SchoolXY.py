import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *



class countPublicSchool(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.publicSchool']
    writes = ['aydenbu_huangyh.zip_PublicSchool_XY']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection

        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        schools = repo['aydenbu_huangyh.publicSchool']

        school_zip_XY = []
        for document in schools.find():
            if document is not None:
                record = {'_id': document['_id'],
                          'zipCode': document['location_zip'],
                          'location': [document['location']['coordinates'][1],
                                       document['location']['coordinates'][0]]
                          }
                school_zip_XY.append(record)

        repo.dropPermanent('zip_PublicSchool_XY')
        repo.createPermanent('zip_PublicSchool_XY')
        repo['aydenbu_huangyh.zip_PublicSchool_XY'].insert_many(school_zip_XY)

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

        this_script = doc.agent('alg:aydenbu_huangyh#zip_PublicSchool_XY',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:school_location',
                              {'prov:label': 'publicSchool Location', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_zip_PublicSchool_XY = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "Get the coordinates of school"})
        doc.wasAssociatedWith(get_zip_PublicSchool_XY, this_script)
        doc.usage(get_zip_PublicSchool_XY, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        zip_PublicSchool_XY = doc.entity('dat:aydenbu_huangyh#zip_publicSchool_XY',
                                         {prov.model.PROV_LABEL: 'Public Schools XY',
                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(zip_PublicSchool_XY, this_script)
        doc.wasGeneratedBy(zip_PublicSchool_XY, get_zip_PublicSchool_XY, endTime)
        doc.wasDerivedFrom(zip_PublicSchool_XY, resource, get_zip_PublicSchool_XY, get_zip_PublicSchool_XY,
                           get_zip_PublicSchool_XY)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


countPublicSchool.execute()
doc = countPublicSchool.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## add provenance
## eof
