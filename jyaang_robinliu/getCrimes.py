import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getCrimes(dml.Algorithm):
    contributor = 'jyaang_robinliu106'
    reads = []
    writes = ['jyaang_robinliu106.crime', 'jyaang_robinliu106.found']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        url = "https://data.cityofboston.gov/api/views/fqn4-4qap/rows.json?accessType=DOWNLOAD"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        crimeData = r['data']
        excludedCrimes = ['Other','Medical Assistance','Towed','Motor Vehicle Accident Response']
        a = []
        for crime in crimeData:
            if crime not in excludedCrimes:
                a.append({"crimeName" : crime[10] , "coord" : crime[-1][1:3] })
        #p = json.dumps(a)

        repo.dropPermanent("crime")
        repo.createPermanent("crime")
        repo['jyaang_robinliu106.crime'].insert_many(a)

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

        this_script = doc.agent('alg:jyaang_robinliu106#getCrimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Crime Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=crime&$select=crimeName,coord'
                }
            )

        crime = doc.entity('dat:jyaang_robinliu106#crime', {prov.model.PROV_LABEL:'Crime Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

getCrimes.execute()
doc = getCrimes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
