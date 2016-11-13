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

class getFirearmsRecovery(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = []
    writes = ['jzhou94_katerin.employee_earnings']

    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        '''
        FIREARMS RECOVERY
        '''
        url = 'https://data.cityofboston.gov/resource/ffz3-2uqv.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("firearms")
        repo.createPermanent("firearms")
        repo['jzhou94_katerin.firearms'].insert_many(r)
    
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

        this_script = doc.agent('alg:getFirearmsRecovery', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_firearms = doc.entity('bdp:ffz3-2uqv', {'prov:label':'Boston Police Department Firearms Recovery Counts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})       
        get_firearms = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 

        doc.wasAssociatedWith(get_firearms, this_script)
        doc.usage(get_firearms, resource_firearms, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?crimegunsrecovered,gunssurrenderedsafeguarded,gunssurrenderedsafeguarded,collectiondate,buybackgunsrecovered'
                }
            )        

        firearms = doc.entity('dat:firearms', {prov.model.PROV_LABEL:'Employee Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(firearms, this_script)
        doc.wasGeneratedBy(firearms, get_firearms, endTime)
        doc.wasDerivedFrom(firearms, resource_firearms, get_firearms, get_firearms, get_firearms)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

getFirearmsRecovery.execute()
print("getFirearmsRecovery Algorithm Done")
doc = getFirearmsRecovery.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("getFirearmsRecovery Provenance Done")

## eof
