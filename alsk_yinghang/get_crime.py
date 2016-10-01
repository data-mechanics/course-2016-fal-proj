import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import numpy as np
token = json.loads(open('../auth.json').read())['token']

class  get_crime(dml.Algorithm):
    contributor = 'alsk_yinghang'
    reads = []
    writes = ['alsk_yinghang.crime']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alsk_yinghang', 'alsk_yinghang')

        print("Downloading data................")
        url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json?$$app_token=%s' % (token)
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        print("Using pandas")
        df = pd.read_json(url)
        new = df[['lat', 'long', 'offense_code_group', 'incident_number']]
        new = new[np.isfinite(new['lat'])]
        new = new[new['lat'] != -1]
        r = json.loads(new.to_json(orient='records'))

        # s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("crime")
        repo.createPermanent("crime")
        print("Trying to add to DB")
        repo['alsk_yinghang.crime'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return

get_crime.execute()
print("DONE!!!!!!!!")