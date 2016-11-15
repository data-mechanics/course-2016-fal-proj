import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getSchools(dml.Algorithm):
    contributor = 'jyaang_robinliu106'
    reads = []
    writes = ['jyaang_robinliu106.school']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        url = "https://data.cityofboston.gov/api/views/e29s-ympv/rows.json?accessType=DOWNLOAD"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        schoolData = r['data']
        a = []
        for school in schoolData:
            #zipcode = school[12][0]
            #zipcode = zipcode[1:-1].split(',')
            #zipcode = str(zipcode[3]).split(':')[1]
            #zipcode = zipcode.strip("\"")
            #a.append({"schoolName" : school[10] , "zipcode" : zipcode , "coord" : school[-1][1:3] })
            a.append({"schoolName" : school[10], "coord" : school[-1][1:3] })

        repo.dropPermanent("school")
        repo.createPermanent("school")
        repo['jyaang_robinliu106.school'].insert_many(a)

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

        this_script = doc.agent('alg:jyaang_robinliu106#getschools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:e29s-ympv', {'prov:label':'School Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_school = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_school, this_script)
        doc.usage(get_school, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )

        school = doc.entity('dat:jyaang_robinliu106#school', {prov.model.PROV_LABEL:'school Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(school, this_script)
        doc.wasGeneratedBy(school, get_school, endTime)
        doc.wasDerivedFrom(school, resource, get_school, get_school, get_school)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

getSchools.execute()
doc = getSchools.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
