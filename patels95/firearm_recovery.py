import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class firearm_recovery(dml.Algorithm):
    contributor = 'patels95'
    reads = []
    writes = ['patels95.firearm_recovery']

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

        # Boston Police Department Firearms Recovery Counts
        url = 'https://data.cityofboston.gov/resource/ffz3-2uqv.json?$$app_token=' + socrataAppToken
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        for i in range(len(r)):
            total = int(r[i]['crimegunsrecovered']) + int(r[i]['gunssurrenderedsafeguarded']) + \
             int(r[i]['buybackgunsrecovered'])
            r[i]['totalgunsrecovered'] = total
            r[i]['collectiondate'] = r[i]['collectiondate'][:10]

        repo.dropPermanent("firearm_recovery")
        repo.createPermanent("firearm_recovery")
        repo['patels95.firearm_recovery'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        print(startTime, endTime)

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

firearm_recovery.execute()
# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
