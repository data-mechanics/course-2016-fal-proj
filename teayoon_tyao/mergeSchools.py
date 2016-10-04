import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pprint

class mergeSchools(dml.Algorithm):
    contributor = 'teayoon_tyao'
    reads = ['teayoon_tyao.publicSchools', 'teayoon_tyao.privateSchools']
    writes = ['teayoon_tyao.schoolsMaster']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('teayoon_tyao', 'teayoon_tyao')

        repo.dropPermanent('teayoon_tyao.schoolsMaster')
        repo.createPermanent('teayoon_tyao.schoolsMaster')

        data = repo.teayoon_tyao.publicSchools.find()

        for document in data:
            publicDict = dict(document)
            for item in publicDict['features']:
                if item['properties']['BLDG_NAME'] and item['geometry']['coordinates']:
                    entry = {'schoolName':item['properties']['BLDG_NAME'], 'latitude':item['geometry']['coordinates'][0], 'longitude':item['geometry']['coordinates'][1], 'type':"public"}
                    res = repo.teayoon_tyao.schoolsMaster.insert_one(entry)

        data = repo.teayoon_tyao.privateSchools.find()

        for document in data:
            privateSchools = dict(document)
            for item in privateSchools['features']:
                if item['properties']['NAME'] and item['geometry']['coordinates']:
                    entry = {'schoolName':item['properties']['NAME'], 'latitude':item['geometry']['coordinates'][0], 'longitude':item['geometry']['coordinates'][1], 'type':'private'}
                    res = repo.teayoon_tyao.schoolsMaster.insert_one(entry)

        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('teayoon_tyao', 'teayoon_tyao')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:teayoon_tyao#mergeSchools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        mergeSCH = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Merge School Data', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(mergeSCH, this_script)
        
        datasets_publicSchools = doc.entity('dat:teayoon_tyao#publicSchools', {'prov:label':'Public Schools', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(mergeSCH, datasets_publicSchools, startTime)

        datasets_privateSchools = doc.entity('dat:teayoon_tyao#privateSchools', {'prov:label':'Private Schools', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(mergeSCH, datasets_privateSchools, startTime)

        datasets_schoolsMaster = doc.entity('dat:teayoon_tyao#schoolsMaster', {'prov:label':'All Schools', prov.model.PROV_TYPE:'ont:Dataset'})

        doc.wasAttributedTo(datasets_schoolsMaster, this_script)
        doc.wasGeneratedBy(datasets_schoolsMaster, mergeSCH, endTime)
        doc.wasDerivedFrom(datasets_schoolsMaster, datasets_publicSchools, mergeSCH, mergeSCH, mergeSCH)
        doc.wasDerivedFrom(datasets_schoolsMaster, datasets_privateSchools, mergeSCH, mergeSCH, mergeSCH)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

mergeSchools.execute()
doc = mergeSchools.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
