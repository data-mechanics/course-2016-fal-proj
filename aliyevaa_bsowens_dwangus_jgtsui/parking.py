
#to install googlemaps: pip3 install -U googlemaps
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
	parking_list=[]
	for elem in data['results']:
		d={}
		d.update({'name':elem['name']})
		d.update(elem['geometry']['location'])
		parking_list.append(d)
	str_parking_list=', '.join(json.dumps(d) for d in parking_list)
	return str_parking_list
	

#privacy violation, but is required for using Google APIs (customized) 
api_key='AIzaSyCKwiWXDPTAHdUFPS9UOQ732-gJ3dCta9w'

gmaps = googlemaps.Client(key=api_key)
google_places=gmaps.places(query='Boston Parking', location='Boston, MA')

pg2='CuQB2wAAAOC0gaeKskZdoXlL0IEZQdq6Goh5V3co-2lV5nXMBn3Xm0-Dfc4NIv0ZTF59i67rb7pYqSYccWnSemirZJm413GwXadgJ6Qp5AZ6FcYEWguNRB1rf3aqAPkafHhLtz14CHnUuXa1MD_xh9dR-ElBR_3zoKRncnAXAC-F0xVKqr-neWXF6RdbpeASczCFjCjFB9aK-8HbZodiGk9siKtm3Cl3Dcc7bv67S_OrIu5PCv55mTQR0cgZgypGcz6ctcwRtZ4G0f9U9cqNajZq2ZYn5peh9P4fMkTfub-sCr7v0iFEEhB___HE7nGPkGsBQg8argV0GhSgF428vvpKPEzHKk__ExzaWGCuzw'
google_places_1=gmaps.places(query='Boston Parking', location='Boston, MA', page_token=pg2)

pg3='CoQDewEAAE-PjFnvJBFH81S5UIGvcthwgD-XauRh9YLD0JRviv5TpiqE7XRAjtRL4igFEMcTSJ_ArAy2OetcEx3W-y9lCRWr9Ghbd2w3yVgBbIBeewe5FsnulsdbRDHrfrSjDOpyzODLLgbueFCE_tcZR8czzmHFf6RwnvyVjdMkFDCr4MgyCSgZZOI5bUWfujMrT6XxhvYACiuwQQVFPjSFP9dxLhO88RgBIV1LHhrK0loruQcH04adVWtzrL04chUVNhlSiL6wv2j_FinWun_ulKwkRegGCfLQGVezgDZ6kZ3LoaRoiyYkmI2qbWrXbPIcqDDJ_tqfJd71U0hRu1NcWavMIUwIcnM1LC99rRSFSX90mo2YeHP5KIC1BBtDSiJ_XP2iUgO82vtz5kzitSbMENf8DmmaFIux1HkHHXvp7IIfhV4I7Jxj4m3B2qKHqsZjL2l8a4W0Rk7Pwkg-qD3EKYIPqUzBcMEIB4L5Q5bT6XOsla3t9AxEMgUPW_1YXCrCtLCPPxIQmeCwG4TP_O08PU8hCW57CxoUXsWiL7vazToAWANu54OF0DkhZhw'

google_places_2=gmaps.places(query='Boston Parking', location='Boston, MA', page_token=pg3)

art_list='['+prep_to_json(google_places)+', '+prep_to_json(google_places_1)+', '+prep_to_json(google_places_2)+']'
r=json.loads(art_list)


class parking(dml.Algorithm):
	contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
	reads = []
	writes = ['aliyevaa_bsowens_dwangus_jgtsui.parking']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(parking.contributor, parking.contributor)
		
		repo.dropPermanent("parking")
		repo.createPermanent("parking")

		repo['aliyevaa_bsowens_dwangus_jgtsui.parking'].insert_many(r)
		repo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client =  dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(parking.contributor,parking.contributor)
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
		doc.add_namespace('dat', 'http://datamechanics.io/data/') 
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/') 
		doc.add_namespace('bdp', 'https://maps.googleapis.com/maps/api/place')

		this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#parking',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource =doc.entity('bdp:city=Boston', {'prov:label':'Boston Parking from Google Maps', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)	
		doc.wasAssociatedWith(get_liquor_data, this_script)

		doc.usage(get_liquor_data , resource, startTime, None)	
		found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#parking', {prov.model.PROV_LABEL:'Boston Parking from Google Maps', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_liquor_data, endTime)
		doc.wasDerivedFrom(found, resource, get_liquor_data, get_liquor_data, get_liquor_data)
		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()
		return doc


parking.execute()
doc = parking.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))






























