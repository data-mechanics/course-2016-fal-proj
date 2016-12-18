import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import scrapePrivateDaycares

class getData(dml.Algorithm):
    contributor = 'aditid_benli95_teayoon_tyao'
    reads = []
    writes = ['aditid_benli95_teayoon_tyao.crimesLegacy', 'aditid_benli95_teayoon_tyao.crimesCurrent', 'aditid_benli95_teayoon_tyao.publicSchools', 'aditid_benli95_teayoon_tyao.privateSchools', 'aditid_benli95_teayoon_tyao.childFeedingPrograms', 'aditid_benli95_teayoon_tyao.dayCamps', 'aditid_benli95_teayoon_tyao.publicDaycares', 'aditid_benli95_teayoon_tyao.privateDaycares']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')
        if (trial == True):
            response = urllib.request.urlopen('https://data.cityofboston.gov/resource/ufcx-3fdn.json?$limit=50').read().decode("utf-8")
        else:
            response = urllib.request.urlopen('https://data.cityofboston.gov/resource/ufcx-3fdn.json?$limit=300000').read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropPermanent("crimesLegacy")
        repo.createPermanent("crimesLegacy")
        repo['aditid_benli95_teayoon_tyao.crimesLegacy'].insert_many(r)
        print('Load crimesLegacy')
        if (trial == True):
            response = urllib.request.urlopen('https://data.cityofboston.gov/resource/29yf-ye7n.json?$limit=50').read().decode("utf-8")
        else:
            response = urllib.request.urlopen('https://data.cityofboston.gov/resource/29yf-ye7n.json?$limit=300000').read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropPermanent("crimesCurrent")
        repo.createPermanent("crimesCurrent")
        repo['aditid_benli95_teayoon_tyao.crimesCurrent'].insert_many(r)
        print('Load crimesCurrent')   

        response = urllib.request.urlopen('http://bostonopendata.boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0.geojson').read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropPermanent("publicSchools")
        repo.createPermanent("publicSchools")
        repo['aditid_benli95_teayoon_tyao.publicSchools'].insert_one(r)
        print('Load publicSchools')   

        response = urllib.request.urlopen('http://bostonopendata.boston.opendata.arcgis.com/datasets/0046426a3e4340a6b025ad52b41be70a_1.geojson').read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropPermanent("privateSchools")
        repo.createPermanent("privateSchools")
        repo['aditid_benli95_teayoon_tyao.privateSchools'].insert_one(r)
        print('Load privateSchools')

        response = urllib.request.urlopen('https://data.cityofboston.gov/resource/6s7x-jq48.json').read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropPermanent("childFeedingPrograms")
        repo.createPermanent("childFeedingPrograms")
        repo['aditid_benli95_teayoon_tyao.childFeedingPrograms'].insert_many(r)
        print('Load childFeedingPrograms')  

        response = urllib.request.urlopen('https://data.cityofboston.gov/resource/jcht-q2ng.json').read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropPermanent("dayCamps")
        repo.createPermanent("dayCamps")
        repo['aditid_benli95_teayoon_tyao.dayCamps'].insert_many(r)
        print('Load dayCamps')        

        response = urllib.request.urlopen('https://data.cityofboston.gov/resource/q6h3-7rpz.json').read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys = True, indent = 2)
        repo.dropPermanent("publicDaycares")
        repo.createPermanent("publicDaycares")
        repo['aditid_benli95_teayoon_tyao.publicDaycares'].insert_many(r)
        print('Load publicDaycares') 

        privateDaycares = scrapePrivateDaycares.getData()
        repo.dropPermanent("privateDaycares")
        repo.createPermanent("privateDaycares")
        repo['aditid_benli95_teayoon_tyao.privateDaycares'].insert_one(privateDaycares)
        print('Load privateDaycares')

        repo.logout()
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
          # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')
        doc.add_namespace('pDc', 'https://www.care.com/day-care/boston-ma-page')



        this_script = doc.agent('alg:aditid_benli95_teayoon_tyao#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_crimesLegacy = doc.entity('cob:ufcx-3fdn', {'prov:label':'Past Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crimesLegacy = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Past Crime Incident Reports', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_crimesLegacy, this_script)
        doc.usage(get_crimesLegacy, resource_crimesLegacy, startTime)

        resource_crimesCurrent = doc.entity('cob:29yf-ye7n', {'prov:label':'Current Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crimesCurrent = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Current Crime Incident Reports', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_crimesCurrent, this_script)
        doc.usage(get_crimesCurrent, resource_crimesCurrent, startTime)

        datasets_publicSchools = doc.entity('bod:1d9509a8b2fd485d9ad471ba2fdb1f90_0', {'prov:label':'Public Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_publicSchools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Public Schools', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_publicSchools, this_script)
        doc.usage(get_publicSchools, datasets_publicSchools, startTime)

        datasets_privateSchools = doc.entity('bod:0046426a3e4340a6b025ad52b41be70a_1', {'prov:label':'Private Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_privateSchools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Private Schools', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_privateSchools, this_script)
        doc.usage(get_privateSchools, datasets_privateSchools, startTime)

        resource_childFeedingPrograms = doc.entity('cob:6s7x-jq48', {'prov:label':'Children Feeding Program', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_childFeedingPrograms = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Children Feeding Program', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_childFeedingPrograms, this_script)
        doc.usage(get_childFeedingPrograms, resource_childFeedingPrograms, startTime)

        resource_dayCamps = doc.entity('cob:jcht-q2ng', {'prov:label':'Day Camps', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_dayCamps = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Day Camps', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_dayCamps, this_script)
        doc.usage(get_dayCamps, resource_dayCamps, startTime)

        resource_publicDaycares = doc.entity('cob:q6h3-7rpz', {'prov:label':'Public Daycares', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_publicDaycares = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Public Daycares', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_publicDaycares, this_script)
        doc.usage(get_publicDaycares, resource_publicDaycares, startTime)

        resource_privateDaycares = doc.entity('pDc:ditid_benli95_teayoon_tyao#privateDaycares', {'prov:label':'Private Daycares', prov.model.PROV_TYPE:'ont:DataResource'})
        get_privateDaycares = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Private Daycares', prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.wasAssociatedWith(get_privateDaycares, this_script)
        doc.usage(get_privateDaycares, resource_privateDaycares, startTime)

        
        crimesLegacy = doc.entity('dat:aditid_benli95_teayoon_tyao#crimesLegacy', {prov.model.PROV_LABEL:'Past Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimesLegacy, this_script)
        doc.wasGeneratedBy(crimesLegacy, get_crimesLegacy, endTime)
        doc.wasDerivedFrom(crimesLegacy, resource_crimesLegacy, get_crimesLegacy, get_crimesLegacy, get_crimesLegacy)

        crimesCurrent = doc.entity('dat:aditid_benli95_teayoon_tyao#crimesCurrent', {prov.model.PROV_LABEL:'Current Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimesCurrent, this_script)
        doc.wasGeneratedBy(crimesCurrent, get_crimesCurrent, endTime)
        doc.wasDerivedFrom(crimesCurrent, resource_crimesCurrent, get_crimesCurrent, get_crimesCurrent, get_crimesCurrent)

        publicSchools = doc.entity('dat:aditid_benli95_teayoon_tyao#publicSchools', {prov.model.PROV_LABEL:'Public Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(publicSchools, this_script)
        doc.wasGeneratedBy(publicSchools, get_publicSchools, endTime)
        doc.wasDerivedFrom(publicSchools, datasets_publicSchools, get_publicSchools, get_publicSchools, get_publicSchools)

        privateSchools = doc.entity('dat:aditid_benli95_teayoon_tyao#privateSchools', {prov.model.PROV_LABEL:'Private Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(privateSchools, this_script)
        doc.wasGeneratedBy(privateSchools, get_privateSchools, endTime)
        doc.wasDerivedFrom(privateSchools, datasets_privateSchools, get_privateSchools, get_privateSchools, get_privateSchools)

        childFeedingPrograms = doc.entity('dat:aditid_benli95_teayoon_tyao#childFeedingPrograms', {prov.model.PROV_LABEL:'Children Feeding Program', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(childFeedingPrograms, this_script)
        doc.wasGeneratedBy(childFeedingPrograms, get_childFeedingPrograms, endTime)
        doc.wasDerivedFrom(childFeedingPrograms, resource_childFeedingPrograms, get_childFeedingPrograms, get_childFeedingPrograms, get_childFeedingPrograms)

        dayCamps = doc.entity('dat:aditid_benli95_teayoon_tyao#dayCamps', {prov.model.PROV_LABEL:'Day Camps', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(dayCamps, this_script)
        doc.wasGeneratedBy(dayCamps, get_dayCamps, endTime)
        doc.wasDerivedFrom(dayCamps, resource_dayCamps, get_dayCamps, get_dayCamps, get_dayCamps)

        publicDaycares = doc.entity('dat:aditid_benli95_teayoon_tyao#publicDaycares', {prov.model.PROV_LABEL:'Public Daycares', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(publicDaycares, this_script)
        doc.wasGeneratedBy(publicDaycares, get_publicDaycares, endTime)
        doc.wasDerivedFrom(publicDaycares, resource_publicDaycares, get_publicDaycares, get_publicDaycares, get_publicDaycares)

        privateDaycares = doc.entity('dat:aditid_benli95_teayoon_tyao#privateDaycares', {prov.model.PROV_LABEL:'Private Daycares', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(privateDaycares, this_script)
        doc.wasGeneratedBy(privateDaycares, get_privateDaycares, endTime)
        doc.wasDerivedFrom(privateDaycares, resource_privateDaycares, get_privateDaycares, get_privateDaycares, get_privateDaycares)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

#getData.execute(True)
getData.execute()
doc = getData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))