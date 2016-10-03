import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'ktan_ngurung'
    reads = []
    writes = ['ktan_ngurung.bigBelly', 'ktan_ngurung.colleges', 'ktan_ngurung.hubways', 'ktan_ngurung.busStops', 'ktan_ngurung.tStops']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')

        repo.dropPermanent("tRidershipLocation")
        repo.createPermanent("tRidershipLocation")
