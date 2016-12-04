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

class getEmployeeEarnings(dml.Algorithm):
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
        
        ''' EMPLOYEE EARNINGS '''
        url = 'https://data.cityofboston.gov/resource/bejm-5s9g.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("employee_earnings")
        repo.createPermanent("employee_earnings")
        repo['jzhou94_katerin.employee_earnings'].insert_many(r)
    
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

        this_script = doc.agent('alg:getEmployeeEarnings', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_employee_earnings = doc.entity('bdp:bejm-5s9g', {'prov:label':'Employee Earnings Report 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'}) 
        get_employee_earnings = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_employee_earnings, this_script)       
        doc.usage(get_employee_earnings, resource_employee_earnings, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?title,retro,injured,postal,details,other,quinn_education_incentive,regular,department_name,name,total_earnings,overtime'
                }
            )

        employee_earnings = doc.entity('dat:employee_earnings', {prov.model.PROV_LABEL:'Employee Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(employee_earnings, this_script)
        doc.wasGeneratedBy(employee_earnings, get_employee_earnings, endTime)
        doc.wasDerivedFrom(employee_earnings, resource_employee_earnings, get_employee_earnings, get_employee_earnings, get_employee_earnings)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

getEmployeeEarnings.execute()
print("getEmployeeEarnings Algorithm Done")
doc = getEmployeeEarnings.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("getEmployeeEarnings Provenance Done")

## eof
