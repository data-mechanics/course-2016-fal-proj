import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pprint
from geopy.geocoders import Nominatim

class mergeChildren(dml.Algorithm):
    contributor = 'aditid_benli95_teayoon_tyao'
    reads = ['aditid_benli95_teayoon_tyao.childFeedingPrograms', 'aditid_benli95_teayoon_tyao.dayCamps', 'aditid_benli95_teayoon_tyao.publicDaycares', 'aditid_benli95_teayoon_tyao.privateDaycares']
    writes = ['aditid_benli95_teayoon_tyao.childFeedingProgramsTrimmed', 'aditid_benli95_teayoon_tyao.dayCampdayCaresMaster']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        geolocator = Nominatim()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        repo.dropPermanent('aditid_benli95_teayoon_tyao.childFeedingProgramsTrimmed')
        repo.createPermanent('aditid_benli95_teayoon_tyao.childFeedingProgramsTrimmed')

        repo.dropPermanent('aditid_benli95_teayoon_tyao.dayCampdayCaresMaster')
        repo.createPermanent('aditid_benli95_teayoon_tyao.dayCampdayCaresMaster')

        data = repo.aditid_benli95_teayoon_tyao.childFeedingPrograms.find()
        for document in data:

            try:
                address = document['stno'] + " " + document['stname'] + " " + document['suffix'] + " " + document['location_1_city']
                location = geolocator.geocode(address)
                latitude = location.latitude
                longitude = location.longitude
            except:
                latitude = None
                longitude = None

            entry = {'name':document['businessname'], 'latitude':latitude, 'longitude':longitude, 'type':'child feeding program'}
            res = repo.aditid_benli95_teayoon_tyao.childFeedingProgramsTrimmed.insert_one(entry)

        data = repo.aditid_benli95_teayoon_tyao.dayCamps.find()
        for document in data:
            try:
                address = document['st_no'] + " " + document['st_name'] + " " +  document['suffix'] + " " + document['location_1_location']
                location = geolocator.geocode(address)
                latitude = location.latitude
                longitude = location.longitude

            except:
                latitude = None
                longitude = None

            entry = {'name':document['business_name'], 'latitude': latitude, 'longitude':longitude, 'type':'day camp'}
            res = repo.aditid_benli95_teayoon_tyao.dayCampdayCaresMaster.insert_one(entry)

        data = repo.aditid_benli95_teayoon_tyao.publicDaycares.find()
        for document in data:
            try:
                address = document['st_no'] + " " + document['st_name'] + " " + document['suffix'] + "Boston, MA"
                location = geolocator.geocode(address)
                latitude = location.latitude
                longitude = location.longitude
            except:
                latitude = None
                longitude = None
            entry = {'name': document['business_name'], 'latitude': latitude, 'longitude': longitude, 'type': 'public daycare'}
            res = repo.aditid_benli95_teayoon_tyao.dayCampdayCaresMaster.insert_one(entry)

        data = repo.aditid_benli95_teayoon_tyao.privateDaycares.find()
        for document in data:
            for doc in document:
                name = doc
                try:
                    latitude = document[doc]['latitude']
                    longitude = document[doc]['longitude']
                except:
                    latitude = None
                    longitude = None
                entry = {'name': name, 'latitude': latitude, 'longitude': longitude, 'type': 'private daycare'}
                res = repo.aditid_benli95_teayoon_tyao.dayCampdayCaresMaster.insert_one(entry)

        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:aditid_benli95_teayoon_tyao#mergeChildren', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        mergeCHI = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Merge Children Establishment Data', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(mergeCHI, this_script)

        trimCFP = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Trim Child Feeding Programs Data', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(trimCFP, this_script)

        resource_childFeedingPrograms = doc.entity('dat:aditid_benli95_teayoon_tyao#childFeedingPrograms', {'prov:label':'Child Feeding Programs', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(trimCFP, resource_childFeedingPrograms, startTime)

        resource_dayCamps = doc.entity('dat:aditid_benli95_teayoon_tyao#dayCamps', {'prov:label':'Day Camps', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(mergeCHI, resource_dayCamps, startTime)

        resource_publicDaycares = doc.entity('dat:aditid_benli95_teayoon_tyao#publicDaycares', {'prov:label':'Public Daycares', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(mergeCHI, resource_publicDaycares, startTime)

        resource_privateDaycares = doc.entity('dat:aditid_benli95_teayoon_tyao#privateDaycares', {'prov:label':'Private Daycares', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(mergeCHI, resource_privateDaycares, startTime)

        childFeedingProgramsTrimmed = doc.entity('dat:aditid_benli95_teayoon_tyao#childFeedingProgramsTrimmed', {'prov:label':'Child Feeding Programs', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.wasAttributedTo(childFeedingProgramsTrimmed, this_script)
        doc.wasGeneratedBy(childFeedingProgramsTrimmed, trimCFP, endTime)
        doc.wasDerivedFrom(childFeedingProgramsTrimmed, resource_childFeedingPrograms, trimCFP, trimCFP, trimCFP)

        dayCampdayCaresMaster = doc.entity('dat:aditid_benli95_teayoon_tyao#dayCampdayCaresMaster', {'prov:label':'Day Camps and Daycares', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.wasAttributedTo(dayCampdayCaresMaster, this_script)
        doc.wasGeneratedBy(dayCampdayCaresMaster, mergeCHI, endTime)
        doc.wasDerivedFrom(dayCampdayCaresMaster, resource_dayCamps, mergeCHI, mergeCHI, mergeCHI)
        doc.wasDerivedFrom(dayCampdayCaresMaster, resource_publicDaycares, mergeCHI, mergeCHI, mergeCHI)
        doc.wasDerivedFrom(dayCampdayCaresMaster, resource_privateDaycares, mergeCHI, mergeCHI, mergeCHI)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

mergeChildren.execute()
doc = mergeChildren.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))