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
        
        print("Using pandas........")
        df = pd.read_json(url)
        new = df[['lat', 'long', 'offense_code_group', 'incident_number']]
        new = new[np.isfinite(new['lat'])]
        new = new[new['lat'] != -1]
        r = json.loads(new.to_json(orient='records'))

        # s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("crime")
        repo.createPermanent("crime")
        print("Trying to add to DB........")
        repo['alsk_yinghang.crime'].insert_many(r)
        print("DONE! Logging out........")
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client =  dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alsk_yinghang', 'alsk_yinghang')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alsk_yinghang#get_crime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)

        crime = doc.entity('dat:alsk_yinghhang#crime', {prov.model.PROV_LABEL:'Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, this_run, endTime)
        doc.wasDerivedFrom(crime, resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

get_crime.execute()
doc = get_crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("DONE WITH GET_CRIME!!!!!!!!")