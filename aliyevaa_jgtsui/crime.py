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


class crime(dml.Algorithm):
    contributor = 'aliyevaa_jgtsui'
    reads = []
    writes = ['aliyevaa_jgtsui.crimeData']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')

        url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json?$$app_token=%s' % dml.auth['services']['cityOfBostonDataPortal']['token']
        
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)   
        repo.dropPermanent("crimeData")   
        repo.createPermanent("crimeData")
        repo['aliyevaa_jgtsui.crimeData'].insert_many(r)

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

        this_script = doc.agent('alg:aliyevaa_jgtsui#crime', 
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime Incidents Report', 
            prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime_data, this_script)

        doc.usage(get_crime_data, resource, startTime, None)

        crimeData = doc.entity('dat:aliyevaa_jgtsui#crimeData', 
            {prov.model.PROV_LABEL:'Crime Data', 
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeData, this_script)
        doc.wasGeneratedBy(crimeData, get_crime_data, endTime)
        doc.wasDerivedFrom(crimeData, resource, get_crime_data,
            get_crime_data, get_crime_data)


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

crime.execute()
doc = crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof