import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *


class merge_crime_zip(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.']
    writes = ['aydenbu_huangyh.crime_zip']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Connect to the Database
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        crimeIncidentReport = repo['aydenbu_huangyh.crime_incident_report']
        zipXY = repo['aydenbu_huangyh.zip_XY']

        # crime_zip = []
        # project_set = {'_id':False, 'day_of_week':True, 'street':True, 'location':True}
        # for document in crimeIncidentReport.find({}, project_set):
        #     # print(document)
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
        return None

merge_crime_zip.execute()