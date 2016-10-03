import json
import dml
import prov.model
import datetime
import pandas as pd
from bson.son import SON

class crime_properties(dml.Algorithm):
    contributor = 'alsk_yinghang'
    reads = ['alsk_yinghang.properties', 'alsk_yinghang.crime']
    writes = ['alsk_yinghang.crime_properties']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alsk_yinghang', 'alsk_yinghang')

        print("Getting to work...............")
        print("Going through every document in crime")
        crime_and_properties = []
        num_collections = repo['alsk_yinghang.crime'].count()
        print(num_collections)
        counter = 1
        for doc in repo['alsk_yinghang.crime'].find():
            results = repo.command(
                    'geoNear', 'alsk_yinghang.properties',
                    near={
                        'type': 'Point',
                        'coordinates': [
                            doc['long'],
                            doc['lat']]},
                    spherical=True,
                    maxDistance=2000)['results']
            sum = 0
            for i in results:
                sum =  sum + i['obj']['gross_tax']
            if len(results) == 0:
                avg = 0
            else:
                avg = sum / len(results)
            jsonObj = doc
            doc['avg_tax'] = avg
            crime_and_properties.append(jsonObj)
            print(counter/num_collections*100)
            counter = counter + 1

        repo.dropPermanent("crime_properties")
        repo.createPermanent("crime_properties")
        repo['alsk_yinghang.crime_properties'].insert_many(crime_and_properties)

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

        this_script = doc.agent(
            'alg:alsk_yinghang#crime_properties', 
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resourceCrime = doc.entity(
            'dat:alsk_yinghang#crime', 
            {'prov:label':'Crime', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        resourceProperties = doc.entity(
            'dat:alsk_yinghang#property_assessment', 
            {'prov:label':'Property Assessment', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resourceCrime, startTime)
        doc.used(this_run, resourceProperties, startTime)

        crime_properties = doc.entity(
            'dat:alsk_yinghhang#crime_properties', 
            {prov.model.PROV_LABEL:'Crime Properties', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_properties, this_script)
        doc.wasGeneratedBy(crime_properties, this_run, endTime)
        doc.wasDerivedFrom(crime_properties, resourceCrime, this_run, this_run, this_run)
        doc.wasDerivedFrom(crime_properties, resourceProperties, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

crime_properties.execute()
doc = crime_properties.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("DONE!!!!!!!!")