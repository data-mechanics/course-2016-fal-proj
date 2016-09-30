import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrieve(dml.Algorithm):
    contributor = 'patels95'
    reads = []
    writes = []

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        # client = dml.pymongo.MongoClient()
        # repo = client.repo
        # repo.authenticate('patels95', 'patels95')

        with open('../auth.json') as jsonFile:
            auth = json.load(jsonFile)
        
        socrataAppToken = auth["socrata"]["app"]

        # Boston Police Department Firearms Recovery Counts
        url = 'https://data.cityofboston.gov/resource/ffz3-2uqv.json?$$app_token=' + socrataAppToken
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r1 = json.loads(response)
        s1 = json.dumps(r1, sort_keys=True, indent=2)
    
        # Crime Incident Reports (July 2012 - August 2015)
        url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json?$$app_token=' + socrataAppToken
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r2 = json.loads(response)
        s2 = json.dumps(r2, sort_keys=True, indent=2)

        # Crime Incident Reports By Weapon Type (July 2012 - August 2015)
        url = 'https://data.cityofboston.gov/resource/vwgc-k7be.json?$$app_token=' + socrataAppToken
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r3 = json.loads(response)
        s3 = json.dumps(r3, sort_keys=True, indent=2)

        # Police Departments
        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r4 = json.loads(response)
        s4 = json.dumps(r4, sort_keys=True, indent=2)

        # Hospital Locations
        url = 'https://data.cityofboston.gov/resource/u6fv-m8v4.json?$$app_token=' + socrataAppToken
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r5 = json.loads(response)
        s5 = json.dumps(r5, sort_keys=True, indent=2)

        print(s5)
        # repo.logout()

        endTime = datetime.datetime.now()

        print(startTime, endTime)

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

retrieve.execute()
# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof