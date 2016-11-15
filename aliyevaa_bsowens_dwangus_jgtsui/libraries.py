import googlemaps
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
import ast

def prep_to_json (data):
	libraries_list=[]
	for elem in data['results']:
		d={}
		d.update({'name':elem['name']})
		d.update(elem['geometry']['location'])
		libraries_list.append(d)
	str_libraries_list=', '.join(json.dumps(d) for d in libraries_list)
	return str_libraries_list
	

#privacy violation, but is required for using Google APIs (customized) 
api_key='AIzaSyCKwiWXDPTAHdUFPS9UOQ732-gJ3dCta9w'

gmaps = googlemaps.Client(key=api_key)
google_places=gmaps.places(query='boston libraries locations', location='Boston, MA')

pg2='CvQB5wAAAJTat3G_zdMHxhK0T6Yi00pi0slM9zxNcaeNUdbVihwDRACdRcelhNU4ixtX9dXwBO0PyJrdGlQQ1JzwXuUBl0qirHobNH7gcN5EM4mRt775X2BoybefBroJ_fcpH1Z06dHMEQerVBAarRyYWfXzsmUrkgyz-yYA2QUWbt7S9vh84r3248RwofawUWnfmvh9BWMMXhqzKfifJeWfWKlnmNFoTsvA8UsfZMILYhtQtAf9fh4YeyTOMIrM_WqPIFrpu24c6KKi8uLrprMPEhZ6hGkbC1deH4sGmn_daSCd354915KabQtmM5veH23t_V6tKBIQaAJ7_s2zgygYbk0XMQsVWBoUQnRn7HOpEWDZo_ly79X8vUoGlVo'
google_places_1=gmaps.places(query='boston libraries locations', location='Boston, MA', page_token=pg2)

art_list='['+prep_to_json(google_places)+', '+prep_to_json(google_places_1)+']'
r=json.loads(art_list)


class libraries(dml.Algorithm):
	contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
	reads = []
	writes = ['aliyevaa_bsowens_dwangus_jgtsui.libraries']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(libraries.contributor, libraries.contributor)
		
		repo.dropPermanent("libraries")
		repo.createPermanent("libraries")
		repo['aliyevaa_bsowens_dwangus_jgtsui.libraries'].insert_many(r)
		for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.libraries.find( modifiers={"$snapshot": True}):
			lng=elem['lng']
			lat=elem['lat']
			repo.aliyevaa_bsowens_dwangus_jgtsui.libraries.update({'_id': elem['_id']}, {'$set': {'location': {'type': 'Point', 'coordinates': [float(lng),float(lat)]}}})
		repo.aliyevaa_bsowens_dwangus_jgtsui.libraries.create_index([('location', '2dsphere')])
		
		repo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client =  dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(libraries.contributor,libraries.contributor)
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
		doc.add_namespace('dat', 'http://datamechanics.io/data/') 
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/') 
		doc.add_namespace('bdp', 'https://maps.googleapis.com/maps/api/place')

		this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#libraries',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource =doc.entity('bdp:city=Boston', {'prov:label':'Boston libraries from Google Maps', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)	
		doc.wasAssociatedWith(get_liquor_data, this_script)

		doc.usage(get_liquor_data , resource, startTime, None)	
		found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#libraries', {prov.model.PROV_LABEL:'Boston Parking from Google Maps', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_liquor_data, endTime)
		doc.wasDerivedFrom(found, resource, get_liquor_data, get_liquor_data, get_liquor_data)
		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()
		return doc


libraries.execute()
doc = libraries.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
