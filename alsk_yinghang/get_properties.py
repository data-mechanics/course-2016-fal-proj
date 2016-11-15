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
    writes = ['alsk_yinghang.properties']

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
        df = df[df['av_total'] != 0]
        zip = df[['longitude', 'latitude', 'av_total']]
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

        repo.dropPermanent("properties")
        repo.createPermanent("properties")
        repo['alsk_yinghang.properties'].ensure_index([('location', dml.pymongo.GEOSPHERE)])
        print("Trying to add to DB....")
        repo['alsk_yinghang.properties'].insert_many(new)
        print("Done. Logging out....")
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

        this_script = doc.agent('alg:alsk_yinghang#get_properties', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:g5b5-xrwi', {'prov:label':'Property Assessment', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)

        property_assessment = doc.entity('dat:alsk_yinghhang#property_assessment', {prov.model.PROV_LABEL:'Property Assessment', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(property_assessment, this_script)
        doc.wasGeneratedBy(property_assessment, this_run, endTime)
        doc.wasDerivedFrom(property_assessment, resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

get_properties.execute()
doc = get_properties.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("Done with get_properties!!!!!!!!")