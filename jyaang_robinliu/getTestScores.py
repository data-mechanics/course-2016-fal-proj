import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class getTestScores(dml.Algorithm):
    contributor = 'jyaang_robinliu106'
    reads = []
    writes = ['jyaang_robinliu106.testScore']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        with open('districtScores.json') as json_data:
            test_scores = json.load(json_data)
        # Change the key "Org Name" to "Name"
        for entry in test_scores:
            for key in entry:
                if key == "Org Name":
                    entry["Name"] = entry.pop(key).upper()
                else:
                    continue



        jsonTestScores = json.dumps(test_scores, sort_keys=True, indent=2)
        repo.dropPermanent("testScores")
        repo.createPermanent("testScores")
        repo['jyaang_robinliu106.testScores'].insert_many(test_scores)

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
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jyaang_robinliu106#getTestScores', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'testScore Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_testScore = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_testScore, this_script)
        doc.usage(get_testScore, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=testScore&$select=type,latitude,longitude,OPEN_DT'
                }
            )

        testScore = doc.entity('dat:jyaang_robinliu106#testScore', {prov.model.PROV_LABEL:'testScore Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(testScore, this_script)
        doc.wasGeneratedBy(testScore, get_testScore, endTime)
        doc.wasDerivedFrom(testScore, resource, get_testScore, get_testScore, get_testScore)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

getTestScores.execute()
doc = getTestScores.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
