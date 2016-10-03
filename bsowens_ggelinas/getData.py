import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getData(dml.Algorithm):
    contributor = 'alice_bob'
    reads = []
    writes = ['bsowens_ggelinas.stations',
              'bsowens_ggelinas.incidents',
              'bsowens_ggelinas.property',
              'bsowens_ggelinas.fio',
              'bsowens_ggelinas.hospitals']

    @staticmethod
    def execute(trial = False):
        '''Retrieve locations of BPD district stations'''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bsowens_ggelinas', 'bsowens_ggelinas')

        dataSets = {
            'stations': 'https://data.cityofboston.gov/resource/pyxn-r3i2.json',
            'incidents': 'https://data.cityofboston.gov/resource/29yf-ye7n.json',
            'property':'https://data.cityofboston.gov/resource/g5b5-xrwi.json',
            'fio':'https://data.cityofboston.gov/resource/2pem-965w.json',
            'hospitals': 'https://data.cityofboston.gov/resource/u6fv-m8v4.json'
        }

        for set in dataSets:
            print(set)
            print(dataSets[set])
            url = dataSets[set]
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            repo.dropPermanent(set)
            repo.createPermanent(set)
            repo['bsowens_ggelinas.' + set].insert_many(r)
            print('done')


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start:startTime", "end:endTime"}

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
        repo.authenticate('bsowens_ggelinas', 'bsowens_ggelinas')


getData.execute()