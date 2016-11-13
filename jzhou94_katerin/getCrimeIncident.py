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

class getCrimeIncident(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = []
    writes = ['jzhou94_katerin.crime_incident']

    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')
        
        ''' CRIME INCIDENTS '''
        url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("crime_incident")
        repo.createPermanent("crime_incident")
        repo['jzhou94_katerin.crime_incident'].insert_many(r)
    
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

        doc.wasAssociatedWith(get_crime_incident, this_script)
        doc.usage(get_crime_incident, resource_crime_incident, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?compnos,naturecode,x,reptdistrict,reportingarea,location,type,weapontype,:@computed_region_aywg_kpfh,ucrpart,year,main_crimecode,streetname,fromdate,domestic,shift,day_week,shooting,y,month,incident_type_description'
                }
            )

        crime_incident = doc.entity('dat:crime_incident', {prov.model.PROV_LABEL:'Crime Incident', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_incident, this_script)
        doc.wasGeneratedBy(crime_incident, get_crime_incident, endTime)
        doc.wasDerivedFrom(crime_incident, resource_crime_incident, get_crime_incident, get_crime_incident, get_crime_incident)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

getCrimeIncident.execute()
print("getCrimeIncident Algorithm Done")
doc = getCrimeIncident.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("getCrimeIncident Provenance Done")

## eof
