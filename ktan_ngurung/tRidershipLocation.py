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

        t_stop_locations = list(repo.ktan_ngurung.tStops.find())
        ridership = repo.ktan_ngurung.find()

        for doc in t_stop_locations:
            docDict = dict(doc)
            for station in docDict['stations']:
                try:
                    print(station['title'])
                except KeyError:
                    print('**** PASSING KEYERROR ****')
                    pass

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')

        pass

example.execute()
doc = example.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
