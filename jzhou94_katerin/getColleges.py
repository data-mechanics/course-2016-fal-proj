'''
CS 591 Project One
projOne.py
jzhou94@bu.edu
katerin@bu.edu
'''
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class getColleges(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = []
    writes = ['jzhou94_katerin.colleges']

    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')
        
        ''' COLLEGES '''
        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("colleges")
        repo.createPermanent("colleges")
        
        repo['jzhou94_katerin.colleges'].insert_one(r)
        print('here')
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
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jzhou94_katerin/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/jzhou94_katerin/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:getCrimeIncident', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_crime_incident = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Crime Incident Reports (July 2012 - August 2015)', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime_incident = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

getColleges.execute()
print("getColleges Algorithm Done")
doc = getColleges.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("getColleges Provenance Done")

## eof
