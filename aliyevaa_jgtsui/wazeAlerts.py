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


class wazeAlerts(dml.Algorithm):
    contributor = 'aliyevaa_jgtsui'
    reads = []
    writes = ['aliyevaa_jgtsui.wazeAlertsData']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')

        url = 'https://data.cityofboston.gov/resource/pvhv-55ac.json?$$app_token=%s' % dml.auth['services']['cityOfBostonDataPortal']['token']
        
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)   
        repo.dropPermanent("wazeAlertsData")   
        repo.createPermanent("wazeAlertsData")
        repo['aliyevaa_jgtsui.wazeAlertsData'].insert_many(r)

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

        this_script = doc.agent('alg:aliyevaa_jgtsui#wazeAlerts', 
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:pvhv-55ac', {'prov:label':'Waze Alerts', 
            prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_wazeAlerts_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_wazeAlerts_data, this_script)

        doc.usage(get_wazeAlerts_data, resource, startTime, None)

        wazeAlertsData = doc.entity('dat:aliyevaa_jgtsui#wazeAlertsData', 
            {prov.model.PROV_LABEL:'Waze Alert Data', 
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(wazeAlertsData, this_script)
        doc.wasGeneratedBy(wazeAlertsData, get_wazeAlerts_data, endTime)
        doc.wasDerivedFrom(wazeAlertsData, resource, get_wazeAlerts_data,
            get_wazeAlerts_data, get_wazeAlerts_data)


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

wazeAlerts.execute()
doc = wazeAlerts.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
