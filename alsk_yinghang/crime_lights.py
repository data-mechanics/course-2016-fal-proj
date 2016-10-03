import json
import dml
import prov.model
import datetime
import pandas as pd
from bson.son import SON
import uuid

class crime_lights(dml.Algorithm):
    contributor = 'alsk_yinghang'
    reads = ['alsk_yinghang.lights', 'alsk_yinghang.crime']
    writes = ['alsk_yinghang.crime_lights']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alsk_yinghang', 'alsk_yinghang')

        print("Getting to work...............")
        print("Going through every document in crime")
        crime_and_lights = []
        for doc in repo['alsk_yinghang.crime'].find():
            length = len(repo.command(
                    'geoNear', 'alsk_yinghang.lights',
                    near={
                        'type': 'Point',
                        'coordinates': [
                            doc['long'],
                            doc['lat']]},
                    spherical=True,
                    maxDistance=20)['results'])
            jsonObj = doc
            doc['num_of_lights'] = length
            crime_and_lights.append(jsonObj)

        repo.dropPermanent("crime_lights")
        repo.createPermanent("crime_lights")
        repo['alsk_yinghang.crime_lights'].insert_many(crime_and_lights)

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
            'alg:alsk_yinghang#crime_lights', 
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resourceCrime = doc.entity(
            'dat:alsk_yinghang#crime', 
            {'prov:label':'Crime', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        resourceLights = doc.entity(
            'dat:alsk_yinghang#streetlight_locations', 
            {'prov:label':'Streetlight locations', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resourceCrime, startTime)
        doc.used(this_run, resourceLights, startTime)

        crime_lights = doc.entity(
            'dat:alsk_yinghhang#crime_lights', 
            {prov.model.PROV_LABEL:'Crime Lights', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_lights, this_script)
        doc.wasGeneratedBy(crime_lights, this_run, endTime)
        doc.wasDerivedFrom(crime_lights, resourceCrime, this_run, this_run, this_run)
        doc.wasDerivedFrom(crime_lights, resourceLights, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

crime_lights.execute()
doc = crime_lights.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("DONE!!!!!!!!")