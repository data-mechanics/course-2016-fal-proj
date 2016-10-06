import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class district_crimes(dml.Algorithm):
    contributor = 'patels95'
    reads = ['patels95.crimes']
    writes = ['patels95.district_crimes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('patels95', 'patels95')

        with open('../auth.json') as jsonFile:
            auth = json.load(jsonFile)

        socrataAppToken = auth["services"]["cityofbostondataportal"]["token"]

        # Police Departments
        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)


        districts = ['A1', 'A15', 'A7', 'B2', 'B3', 'C6', 'C11', 'D4', 'D14', 'E5', 'E13', 'E18']
        data = []

        # count the number of firearm crimes in each district
        for d in districts:
            count = 0
            for crime in repo['patels95.crimes'].find():
                if d == crime['reptdistrict']:
                    count += 1
            data.append({'district': d, 'total_crimes': count})

        for d in data:
            for dept in r['features']:
                # name is the district code taken from the full name of the department
                name = dept['properties']['NAME'][8:13].replace('-','')
                if d['district'] in name:
                    d['bpd_id'] = dept['properties']['BPD_ID']
                    d['address'] = dept['properties']['ADDRESS']
                    d['zip'] = dept['properties']['ZIP']
                    d['geometry'] = dept['geometry']


        repo.dropPermanent("district_crimes")
        repo.createPermanent("district_crimes")
        repo['patels95.district_crimes'].insert_many(data)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('patels95', 'patels95')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/')

        this_script = doc.agent('alg:patels95#district_crimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:patels95#crimes', {'prov:label':'Crimes', prov.model.PROV_TYPE:'ont:DataResource'})
        resource2 = doc.entity('bod:e5a0066d38ac4e2abbc7918197a4f6af_6', {'prov:label':'Police Departments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource1, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_run, resource2, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})


        district_crimes_count = doc.entity('dat:patels95#district_crimes', {prov.model.PROV_LABEL:'District Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(district_crimes_count, this_script)
        doc.wasGeneratedBy(district_crimes_count, this_run, endTime)
        doc.wasDerivedFrom(district_crimes_count, resource1, this_run, this_run, this_run)
        doc.wasDerivedFrom(district_crimes_count, resource2, this_run, this_run, this_run)

        repo.record(doc.serialize())
        repo.logout()

        return doc

district_crimes.execute()
doc = district_crimes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
