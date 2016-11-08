"""
Assel Aliyeva (aliyeeva@bu.edu), Jennifer Tsui (jgtsui@bu.edu)
aliyeeva_jgtsui
October 3, 2016

CS 591 L1 - Data Mechanics
Andrei Lapets (lapets@bu.edu)
Boston University

Project #1 -- Data Retrieval, Storage, Provenance, Transformations
"""

from urllib import request
import urllib
import json
import dml
import prov.model
import datetime
import uuid


class properties2016(dml.Algorithm):
    contributor = 'aliyevaa_jgtsui'
    reads = []
    writes = ['aliyevaa_jgtsui.properties2016Data']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')

        url = 'https://data.cityofboston.gov/resource/g5b5-xrwi.json?$$app_token=%s' % dml.auth['services']['cityOfBostonDataPortal']['token']
        
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)   
        repo.dropPermanent("properties2016Data")   
        repo.createPermanent("properties2016Data")
        repo['aliyevaa_jgtsui.properties2016Data'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aliyevaa_jgtsui') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/aliyevaa_jgtsui') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyevaa_jgtsui#properties2016', 
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:g5b5-xrwi', {'prov:label':'Property Assessment 2016', 
            prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_properties2016_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_properties2016_data, this_script)

        doc.usage(get_properties2016_data, resource, startTime, None)

        properties2016Data = doc.entity('dat:aliyevaa_jgtsui#properties2016Data', 
            {prov.model.PROV_LABEL:'properties 2016 data', 
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(properties2016Data, this_script)
        doc.wasGeneratedBy(properties2016Data, get_properties2016_data, endTime)
        doc.wasDerivedFrom(properties2016Data, resource, get_properties2016_data,
            get_properties2016_data, get_properties2016_data)


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

properties2016.execute()
doc = properties2016.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof