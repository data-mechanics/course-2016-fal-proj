import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sys

contributor = 'dichgao'
reads = ['311']
writes = ['neighborhood_locations']

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('dichgao', 'dichgao')


db = repo['dichgao.311']
startTime = datetime.datetime.now()

#extract geolocation with neighborhood 
neighborhood_locations = []
arr = []
for doc in db.find():
    if 'longitude' in doc and 'latitude' in doc and 'neighborhood' in doc:
        neighbor = doc['neighborhood']
        longitude = doc['longitude']
        i = longitude.find('.')
        if i != -1:
            longitude = longitude[:i+4]	

        latitude = doc['latitude']
        i = latitude.find('.')
        if i != -1:
            latitude = latitude[:i+4]
            neighbor_geo = {
			'neighborhood': neighbor,
			'longitude':	float(longitude),
			'latitude':	float(latitude)
		}

        neighborhood_locations.append(neighbor_geo)

repo.dropTemporary("neighborhood_locations")
repo.createTemporary("neighborhood_locations")
repo['dichgao.neighborhood_locations'].insert_many(neighborhood_locations)

endTime = datetime.datetime.now()

#provenance
startTime = None
endTime = None
doc = prov.model.ProvDocument()

doc.add_namespace('alg', 'http://datamechanics.io/algorithm/dichgao') # The scripts are in <folder>#<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/dichgao') # The data sets are in <user>#<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:dichgao#neighbor_locations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource = doc.entity('dat:311', {prov.model.PROV_LABEL:'311', prov.model.PROV_TYPE:'ont:DataSet'})
get_neighborhood_locations = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime)
doc.wasAssociatedWith(get_neighborhood_locations, this_script)
doc.usage(get_neighborhood_locations, resource, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )

neighborhood_locations = doc.entity('dat:dichgao#neighborhood_locations', {prov.model.PROV_LABEL:'Neighborhood Locations', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(neighborhood_locations, this_script)
doc.wasGeneratedBy(neighborhood_locations, get_neighborhood_locations, endTime)
doc.wasDerivedFrom(neighborhood_locations, resource, get_neighborhood_locations, get_neighborhood_locations, get_neighborhood_locations)

repo.record(doc.serialize()) # Record the provenance document.

print(doc.get_provn())

repo.logout()
