import json
import dml
import prov.model
import datetime
import pandas as pd
from bson.son import SON

class crime_police_stations(dml.Algorithm):
    contributor = 'alsk_yinghang'
    reads = ['alsk_yinghang.police_stations', 'alsk_yinghang.crime']
    writes = ['alsk_yinghang.crime_police_stations']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alsk_yinghang', 'alsk_yinghang')

        print("Getting to work...............")
        print("Going through every document in crime")
        crime_and_police = []
        for doc in repo['alsk_yinghang.crime'].find():
            dist = repo.command(
                    'geoNear', 'alsk_yinghang.police_stations',
                    near={
                        'type': 'Point',
                        'coordinates': [
                            doc['long'],
                            doc['lat']]},
                    spherical=True,
                    num=1)['stats']['maxDistance']
            jsonObj = doc
            doc['distance_nearest_police_station'] = dist
            crime_and_police.append(jsonObj)

        repo.dropPermanent("crime_police_station")
        repo.createPermanent("crime_police_station")
        repo['alsk_yinghang.crime_police_station'].insert_many(crime_and_police)

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
            'alg:alsk_yinghang#crime_police_station', 
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resourceCrime = doc.entity(
            'dat:alsk_yinghang#crime', 
            {'prov:label':'Crime', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        resourcePolice = doc.entity(
            'dat:alsk_yinghang#police_stations', 
            {'prov:label':'Police Stations', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resourceCrime, startTime)
        doc.used(this_run, resourcePolice, startTime)

        crime_police_station = doc.entity(
            'dat:alsk_yinghhang#crime_police_station', 
            {prov.model.PROV_LABEL:'Crime Police Station', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_police_station, this_script)
        doc.wasGeneratedBy(crime_police_station, this_run, endTime)
        doc.wasDerivedFrom(crime_police_station, resourceCrime, this_run, this_run, this_run)
        doc.wasDerivedFrom(crime_police_station, resourcePolice, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

crime_police_stations.execute()
doc = crime_police_stations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("DONE!!!!!!!!")