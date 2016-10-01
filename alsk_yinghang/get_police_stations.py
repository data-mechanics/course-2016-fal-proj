import urllib.request
import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid
token = json.loads(open('../auth.json').read())['token']

class  get_police_stations(dml.Algorithm):
    contributor = 'alsk_yinghang'
    reads = []
    writes = ['alsk_yinghang.police_stations']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alsk_yinghang', 'alsk_yinghang')

        print("Downloading data................")
        url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json?$$app_token=%s' % (token)

        print("Using pandas")
        df = pd.read_json(url)
        zip = df[['location_zip', 'name', 'location']]
        r = json.loads(zip.to_json(orient='records'))
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r1 = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)

        # print(r1)
        print(r)
        repo.dropPermanent("police_stations")
        repo.createPermanent("police_stations")
        repo['alsk_yinghang.police_stations'].ensure_index([("location", dml.pymongo.GEOSPHERE)])
        print("Dataframe")
        print(df)
        print("Trying to add to DB")
        repo['alsk_yinghang.police_stations'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return

get_police_stations.execute()
print("DONE!!!!!!!!")