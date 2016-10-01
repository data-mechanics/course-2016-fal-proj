import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class district_crimes(dml.Algorithm):
    contributor = 'patels95'
    reads = ['patels95.crimes']
    writes = ['patels95.district_crimes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('patels95', 'patels95')

        with open('../auth.json') as jsonFile:
            auth = json.load(jsonFile)

        socrataAppToken = auth["socrata"]["app"]

        districts = ['A1', 'A15', 'A7', 'B2', 'B3', 'C6', 'C11', 'D4', 'D14', 'E5', 'E13', 'E18']
        data = []

        # count the number of firearm crimes in each district
        for d in districts:
            count = 0
            for crime in repo['patels95.crimes'].find():
                if d == crime['reptdistrict']:
                    count += 1
            data.append({'district': d, 'total_crimes': count})

        repo.dropPermanent("district_crimes")
        repo.createPermanent("district_crimes")
        repo['patels95.district_crimes'].insert_many(data)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

district_crimes.execute()
# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
