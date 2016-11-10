import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *

'''
TO get the approximated coordinate in center of a zipcode area
By using the approved_building_permits
'''


class zipXY(dml.Algorithm):





    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.approved_building_permits']
    writes = ['aydenbu_huangyh.zipXY']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Connect to the Database
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        buildingPermits = repo['aydenbu_huangyh.approved_building_permits']


        # Map reduce
        # mapper = Code("""
        #                 function() {
        #                     emit(this.zip, {longitude: this.location.longitude, latitude: this.location.latitude})
        #                 }
        #               """)
        # reducer = Code("""
        #                 function(k, vs) {
        #
        #                 }
        #                 """
        # )
        # repo.dropPermanent()
        # result = buildingPermits.map_reduce(mapper, reducer, "aydenbu_huangyh.zip_crime")

        ##########################

        # pipeline = [{"$project": { "zip":"$zip", "location.latitude": True, "location.longitude": True}}]
        #
        # zip_XY = list(buildingPermits.aggregate(pipeline))
        # print(zip_XY)
        # repo.dropPermanent("zip_XY")
        # repo.createPermanent("zip_XY")
        # repo['zip_XY'].insert_many(zip_XY)

        #############################
        # Helper Methods
        def aggregate(R, f):
            keys = {r['zip'] for r in R}
            return [{'zip': key, 'latitude': f([r for r in R if r['zip'] == key])[0],
                     'longtitude': f([r for r in R if r['zip'] == key])[1]} for key in keys]

        def plus(args):
            p = [0, 0]
            c = len(args)
            for i in args:
                p[0] += i['latitude']
                p[1] += i['longitude']
            p[0] /= c
            p[1] /= c
            return tuple(p)

            ################################


        zips_locations = []
        for document in buildingPermits.find():
            if 'location' in document and 'zip' in document:
                zipcode = document['zip']
                longitude = document['location']['longitude']
                i = longitude.find('.')
                if i != -1:
                    longitude = longitude[:i + 4]  # rounded to three decimals
                latitude = document['location']['latitude']
                i = latitude.find('.')
                if i != -1:
                    latitude = latitude[:i + 4]  # rounded to three decimals
                zl = {
                    'zip': zipcode,
                    'longitude': float(longitude),
                    'latitude': float(latitude)
                }

                zips_locations.append(zl)

        # print(zips_locations)
        print(zips_locations)
        zips_locations = aggregate(zips_locations, plus)


        repo.dropPermanent("zip_XY")
        repo.createPermanent("zip_XY")
        repo['aydenbu_huangyh.zip_XY'].insert_many(zips_locations)



        # ###########





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

        this_script = doc.agent('alg:aydenbu_huangyh#zipXY',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:approved_building_permit',
                              {'prov:label': 'Approved Building Permit', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_zip_XY = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                         {
                                                             prov.model.PROV_LABEL: "Count the number of CornerStores in each zip"})
        doc.wasAssociatedWith(get_zip_XY, this_script)
        doc.usage(get_zip_XY, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        zip_XY = doc.entity('dat:aydenbu_huangyh#zip_Healthycornerstores_count',
                                                   {prov.model.PROV_LABEL: 'Healthy Corner Stores Count',
                                                    prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(zip_XY, this_script)
        doc.wasGeneratedBy(zip_XY, get_zip_XY, endTime)
        doc.wasDerivedFrom(zip_XY, resource, get_zip_XY,
                           get_zip_XY,
                           get_zip_XY)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc

zipXY.execute()

