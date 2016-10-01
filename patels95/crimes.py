import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crimes(dml.Algorithm):
    contributor = 'patels95'
    reads = []
    writes = ['patels95.crimes']

    def select(R, s):
        return [t for t in R if s(t)]

    # return true if the weapontype is a firearm
    def firearm_only(crime):
        if crime['weapontype'] == 'Firearm':
            return True
        else:
            return False

    # return true if the crime date is in the time period
    def date_check(crime):
        startDate = datetime.datetime.strptime('2014-08-20', '%Y-%m-%d')
        endDate = datetime.datetime.strptime('2015-07-27', '%Y-%m-%d')
        crimeDate = datetime.datetime.strptime(crime['fromdate'][:10], '%Y-%m-%d')

        if crimeDate >= startDate and crimeDate <= endDate:
            return True
        else:
            return False


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
        url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json?$$app_token=' + socrataAppToken
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        data = crimes.select(r, crimes.firearm_only)

        data = crimes.select(data, crimes.date_check)

        repo.dropPermanent("crimes")
        repo.createPermanent("crimes")
        repo['patels95.crimes'].insert_many(data)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

crimes.execute()
# doc = retrieve.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
