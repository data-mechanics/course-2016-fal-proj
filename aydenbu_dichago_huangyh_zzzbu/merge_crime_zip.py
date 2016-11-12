import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *


# Merge Crime to the related zipcode area

class merge_crime_zip(dml.Algorithm):




    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.crime_incident_report', 'aydenbu_huangyh.zip_XY']
    writes = ['aydenbu_huangyh.crime_zip']

    @staticmethod
    def execute(trial = False):

        # Helper Method
        def dist(p, q):
            (x1, y1) = p
            (x2, y2) = q
            return (x1 - x2) ** 2 + (y1 - y2) ** 2

            #########################

        startTime = datetime.datetime.now()

        # Connect to the Database
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        crimeIncidentReport = repo['aydenbu_huangyh.crime_incident_report']
        zip_XY = repo['aydenbu_huangyh.zip_XY']

        crime_zip = []
        project_set = {'_id':False, 'day_of_week':True, 'street':True, 'location':True}
        for document in crimeIncidentReport.find({}, project_set):
            print(document)
            longitude, latitude = document['location']['coordinates'][0], document['location']['coordinates'][1]
            crimeXY = tuple((longitude, latitude))
            if longitude != 0 and latitude != 0:
                smallestDist = 100*100
                crime_zip_each = {'longitude': crimeXY[0], 'latitude': crimeXY[1], 'num': 1}

                for document2 in zip_XY.find():
                    zipLong, zipLat = document2['longitude'], document2['latitude']
                    zipXY = tuple((zipLong, zipLat))
                    if dist(crimeXY, zipXY) <= smallestDist:
                        smallestDist = dist(crimeXY, zipXY)
                        crime_zip_each['zip'] = document2['zip']
                        # crime_zip_each['longitude'], crime_zip_each['latitude'] = crimeXY[0], crimeXY[1]
                        # crime_zip_each['num'] = 1

                crime_zip.append(crime_zip_each)

        repo.dropPermanent('crime_zip')
        repo.createPermanent('crime_zip')
        repo['aydenbu_huangyh.crime_zip'].insert_many(crime_zip)


        #     zipcode = zipXY.find_one({'longitude':document['location']['coordinates'][0],
        #                                'latitude':document['location']['coordinates'][1]},
        #                              {'_id':False, 'zip':True})
        #     print(zipcode)
        #     if zipcode is None:
        #         continue
        #     else:
        #         document['zip'] = zipcode['zip']
        #         crime_zip.append(document)

        # repo.dropPermanent('crime_zip')
        # repo.createPermanent('crime_zip')
        # repo['aydenbu_huangyh.crime_zip'].insert_many(crime_zip)




        repo.logout()
        endTime = datetime.datetime.now()


        return {'start':startTime, 'end':endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
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
        resource = doc.entity('dat:crime_incident_report',
                              {'prov:label': 'Crime Incident Report', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resource2 = doc.entity('dat:zip_XY',
                              {'prov:label': 'The center coordinates of each zip', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        merge_crime_zip = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                  {
                                      prov.model.PROV_LABEL: "Count the number of CornerStores in each zip"})
        doc.wasAssociatedWith(merge_crime_zip, this_script)
        doc.usage(merge_crime_zip, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(merge_crime_zip, resource2, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        crime_zip = doc.entity('dat:aydenbu_huangyh#zip_Healthycornerstores_count',
                            {prov.model.PROV_LABEL: 'Healthy Corner Stores Count',
                             prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crime_zip, this_script)
        doc.wasGeneratedBy(crime_zip, merge_crime_zip, endTime)
        doc.wasDerivedFrom(crime_zip, resource, merge_crime_zip,
                           merge_crime_zip,
                           merge_crime_zip)
        doc.wasDerivedFrom(crime_zip, resource2, merge_crime_zip,
                           merge_crime_zip,
                           merge_crime_zip)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc

merge_crime_zip.execute()