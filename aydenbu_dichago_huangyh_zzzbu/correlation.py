import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *


class correlation(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.public_earning_crime_boston']
    writes = ['']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # Connect to the Database
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        data = repo['aydenbu_huangyh.public_earning_crime_boston']

        avgs = []
        stores = []
        hospitals = []
        schools = []
        gardens = []
        crimes = []
        for document in data.find():
            avg = tuple((document['_id'], document['value']['avg']))
            store = tuple((document['_id'], document['value']['numofStore']))
            hospital = tuple((document['_id'], document['value']['numofHospital']))
            school = tuple((document['_id'], document['value']['numofSchool']))
            garden = tuple((document['_id'], document['value']['numofGarden']))
            crime = tuple((document['_id'], document['value']['numofCrime']))
            avgs.append(avg)
            stores.append(store)
            hospitals.append(hospital)
            schools.append(school)
            gardens.append(garden)
            crimes.append(crime)




        # print(avgs)
        # print(stores)
        # print(hospitals)
        # print(schools)
        # print(gardens)
        # print(crimes)
        '''
        Implement the statistic methods here:
        '''








        '''
        Statistic methods end Here
        '''







        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''
        return None


correlation.execute()
