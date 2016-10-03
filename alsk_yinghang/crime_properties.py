import json
import dml
import prov.model
import datetime
import pandas as pd
from bson.son import SON

class crime_properties(dml.Algorithm):
    contributor = 'alsk_yinghang'
    reads = ['alsk_yinghang.properties', 'alsk_yinghang.crime']
    writes = ['alsk_yinghang.crime_properties']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alsk_yinghang', 'alsk_yinghang')

        print("Getting to work...............")
        print("Going through every document in crime")
        crime_and_properties = []
        num_collections = repo['alsk_yinghang.crime'].count()
        print(num_collections)
        counter = 1
        for doc in repo['alsk_yinghang.crime'].find():
            results = repo.command(
                    'geoNear', 'alsk_yinghang.properties',
                    near={
                        'type': 'Point',
                        'coordinates': [
                            doc['long'],
                            doc['lat']]},
                    spherical=True,
                    maxDistance=2000)['results']
            sum = 0
            for i in results:
                sum =  sum + i['obj']['gross_tax']
            if len(results) == 0:
                avg = 0
            else:
                avg = sum / len(results)
            jsonObj = doc
            doc['avg_tax'] = avg
            crime_and_properties.append(jsonObj)
            print(counter/num_collections*100)
            counter = counter + 1

        repo.dropPermanent("crime_properties")
        repo.createPermanent("crime_properties")
        repo['alsk_yinghang.crime_properties'].insert_many(crime_and_properties)

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return

crime_properties.execute()
print("DONE!!!!!!!!")