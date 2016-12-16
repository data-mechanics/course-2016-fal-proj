import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getHospitals(dml.Algorithm):
    contributor = 'jyaang_robinliu106'
    reads = []
    writes = ['jyaang_robinliu106.hospital']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        url = "https://data.cityofboston.gov/api/views/46f7-2snz/rows.json?accessType=DOWNLOAD"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        hospitalData = r['data']
        a = []
        for hospital in hospitalData:
            city = hospital[14][0]
            city = city[1:-1].split(',')
            city = str(city[1]).split(':')[1]
            city = city.strip("\"")
            #a.append({"hospitalName" : hospital[8] , "city" : city , "coord" : hospital[-1][1:3] })
            a.append({"hospitalName" : hospital[8] , "coord" : hospital[-1][1:3] })

        repo.dropPermanent("hospital")
        repo.createPermanent("hospital")
        repo['jyaang_robinliu106.hospital'].insert_many(a)

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

        this_script = doc.agent('alg:jyaang_robinliu106#getHospitals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:46f7-2snz', {'prov:label':'Hospital Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_hospital = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hospital, this_script)
        doc.usage(get_hospital, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )

        hospital = doc.entity('dat:jyaang_robinliu106#hospital', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospital, this_script)
        doc.wasGeneratedBy(hospital, get_hospital, endTime)
        doc.wasDerivedFrom(hospital, resource, get_hospital, get_hospital, get_hospital)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

getHospitals.execute()
doc = getHospitals.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
