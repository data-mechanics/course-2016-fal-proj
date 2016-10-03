import urllib.request
import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid
token = json.loads(open('../auth.json').read())['token']

class  get_properties(dml.Algorithm):
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
        url = 'https://data.cityofboston.gov/resource/g5b5-xrwi.json?$$app_token=%s' % (token)

        print("Using pandas")
        df = pd.read_json(url)
        df = df[df['latitude'] != "#N/A"]
        df = df[df['gross_tax'] != 0]
        zip = df[['longitude', 'latitude', 'gross_tax']]
        r = json.loads(zip.to_json(orient='records'))
        new = []
        for obj in r:
            loc = {
                "type": "Point",
                "coordinates": [float(obj['longitude']), float(obj['latitude'])]
            }
            obj['location'] = loc
            new.append(obj)
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r1 = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)

        # print(r1)
        print("Print result")
        print(r)
        repo.dropPermanent("properties")
        repo.createPermanent("properties")
        repo['alsk_yinghang.properties'].ensure_index([('location', dml.pymongo.GEOSPHERE)])
        print("Trying to add to DB")
        repo['alsk_yinghang.properties'].insert_many(new)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return

get_properties.execute()
print("DONE!!!!!!!!")