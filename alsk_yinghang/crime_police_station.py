import json
import dml
import prov.model
import datetime
import pandas as pd
from bson.son import SON

class crime_police_stations(dml.Algorithm):
    contributor = 'alsk_yinghang'
    reads = ['alsk_yinghang.police_stations', 'alsk_yinghang.crime']
    writes = ['alsk_yinghang.crime_police_stations']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alsk_yinghang', 'alsk_yinghang')

        print("Getting to work...............")
        print("Going through every document in crime")
        crime_and_police = []
        for doc in repo['alsk_yinghang.crime'].find():
            dist = repo.command(
                    'geoNear', 'alsk_yinghang.police_stations',
                    near={
                        'type': 'Point',
                        'coordinates': [
                            doc['long'],
                            doc['lat']]},
                    spherical=True,
                    num=1)['stats']['maxDistance']
            jsonObj = doc
            doc['distance_nearest_police_station'] = dist
            crime_and_police.append(jsonObj)

        repo.dropPermanent("crime_police_station")
        repo.createPermanent("crime_police_station")
        repo['alsk_yinghang.crime_police_station'].insert_many(crime_and_police)

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return

crime_police_stations.execute()
print("DONE!!!!!!!!")