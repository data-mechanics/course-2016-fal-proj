import json
import dml
import prov.model
import datetime
import pandas as pd
from bson.son import SON

class crime_lights(dml.Algorithm):
    contributor = 'alsk_yinghang'
    reads = ['alsk_yinghang.lights', 'alsk_yinghang.crime']
    writes = ['alsk_yinghang.crime_lights']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alsk_yinghang', 'alsk_yinghang')

        print("Getting to work...............")
        print("Going through every document in crime")
        crime_and_lights = []
        for doc in repo['alsk_yinghang.crime'].find():
            length = len(repo.command(
                    'geoNear', 'alsk_yinghang.lights',
                    near={
                        'type': 'Point',
                        'coordinates': [
                            doc['long'],
                            doc['lat']]},
                    spherical=True,
                    maxDistance=20)['results'])
            jsonObj = doc
            doc['num_of_lights'] = length
            crime_and_lights.append(jsonObj)

        repo.dropPermanent("crime_lights")
        repo.createPermanent("crime_lights")
        repo['alsk_yinghang.crime_lights'].insert_many(crime_and_lights)

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return

crime_lights.execute()
print("DONE!!!!!!!!")