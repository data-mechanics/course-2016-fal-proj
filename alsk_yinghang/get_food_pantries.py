import urllib.request
import json
import dml
import prov.model
import datetime
import pandas as pd
import uuid
token = json.loads(open('../auth.json').read())['token']

class  get_food_pantries(dml.Algorithm):
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
        url = 'https://data.cityofboston.gov/resource/4tie-bhxw.json?$$app_token=%s' % (token)

        print("Using pandas")
        df = pd.read_json(url)
        zip = df[['location_1', 'name', 'area']]
        zip.columns = ['location', 'name', 'area']
        r = json.loads(zip.to_json(orient='records'))
        # response = urllib.request.urlopen(url).read().decode("utf-8")
        # r1 = json.loads(response)
        # s = json.dumps(r, sort_keys=True, indent=2)

        # print(r1)
        print(r)
        repo.dropPermanent("food_pantries")
        repo.createPermanent("food_pantries")
        repo['alsk_yinghang.food_pantries'].ensure_index([("location", dml.pymongo.GEOSPHERE)])
        print("Dataframe")
        print(df)
        print("Trying to add to DB")
        repo['alsk_yinghang.food_pantries'].insert_many(r)

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

        this_script = doc.agent('alg:alsk_yinghang#get_food_pantries', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:4tie-bhxw', {'prov:label':'Food Pantries', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)

        food_pantries = doc.entity('dat:alsk_yinghhang#food_pantries', {prov.model.PROV_LABEL:'Food Pantries', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(food_pantries, this_script)
        doc.wasGeneratedBy(food_pantries, this_run, endTime)
        doc.wasDerivedFrom(food_pantries, resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

get_food_pantries.execute()
doc = get_properties.provenance()
print("DONE!!!!!!!!")