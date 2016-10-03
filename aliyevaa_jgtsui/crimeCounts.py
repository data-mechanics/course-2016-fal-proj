"""
Assel Aliyeva (aliyeeva@bu.edu), Jennifer Tsui (jgtsui@bu.edu)
aliyeeva_jgtsui
October 3, 2016

CS 591 L1 - Data Mechanics
Andrei Lapets (lapets@bu.edu)
Boston University

Project #1 -- Data Retrieval, Storage, Provenance, Transformations
"""

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

## add python relational methods?

class crimeCounts(dml.Algorithm):
    contributor = 'aliyeeva_jgtsui'
    reads = []
    writes = ['aliyeeva_jgtsui.crimeCounts']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aliyeeva_jgtsui', 'aliyeeva_jgtsui')

        # retrieve crime incidence dataset
        url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)   
        repo.dropPermanent("crimeCountsLocations")   
        repo.createPermanent("crimeCountsLocations")
        repo['aliyeeva_jgtsui.crimeCountsLocations'].insert_many(r)

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
        repo.authenticate('aliyeeva_jgtsui', 'aliyeeva_jgtsui')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aliyeeva_jgtsui') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/aliyeeva_jgtsui') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyeeva_jgtsui#crimeCounts', 
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime Incidents Report', 
            prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime_counts_locations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime_counts_locations, this_script)

        doc.usage(get_crime_counts_locations, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT' #CHANGE THISSSSS
                }
            )

        crimeCountsLocations = doc.entity('dat:aliyeeva_jgtsui#crimeCountsLocations', 
            {prov.model.PROV_LABEL:'Crime Count for Locations', 
            prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeCountsLocations, this_script)
        doc.wasGeneratedBy(crimeCountsLocations, get_crime_counts_locations, endTime)
        doc.wasDerivedFrom(crimeCountsLocations, resource, get_crime_counts_locations,
            get_crime_counts_locations, get_crime_counts_locations)


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

crimeCounts.execute()
doc = crimeCounts.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof