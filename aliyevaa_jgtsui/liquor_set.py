#import requrests
import urllib.request
import dml
import json
import pymongo
import prov.model
import uuid
import datetime
from geopy.geocoders import Nominatim

#from functools import reduce

from pymongo import MongoClient


class liquor_stores(dml.Algorithm):
	contributor = 'aliyevaa_jgtsui'
	reads = []
	writes = ['aliyevaa_jgtsui.liquor_set']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = MongoClient()
		mrepo = client.repo
		mrepo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')
		
		mrepo.dropPermanent("liquor_data")
		mrepo.createPermanent("liquor_data")
				
		url = 'https://data.cityofboston.gov/resource/g9d9-7sj6.json?$$app_token=%s' % dml.auth['services']['cityOfBostonDataPortal']['token']	
		data = urllib.request.urlopen(url).read().decode('utf8')
		djson = json.loads(data)
		s=json.dumps(djson, sort_keys=True, indent=2)
		geolocator = Nominatim()
		'''
		for entry in djson:
			location=entry['location']
			if location['latitude']=='0.0':
				djson.remove(entry)		
			else:
				a=[]
				strLoc=location['latitude']+' , '+ location['longitude']
				location = geolocator.reverse(strLoc)
				try:
					addr=location.address
					a=addr.split(',')
					try:
						del entry ['location']
						entry['zip']=a[-2]
						#print(entry)
					except:
						djson.remove(entry)
						#if (isinstance(int(a[-2]), int))
				except:		
						pass
	
		'''	
		mrepo['aliyevaa_jgtsui.liquor_data'].insert_many(djson)
		mrepo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}	
	
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = MongoClient()
		repo = client.repo
		repo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aliyevaa_jgtsui')
		doc.add_namespace('dat', 'http://datamechanics.io/data/aliyevaa_jgtsui') 
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/') 
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:aliyevaa_jgtsui#liquor_set',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource =doc.entity('bdp:g9d9-7sj6', {'prov:label':'Liquor Licenses', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)	
		doc.wasAssociatedWith(get_liquor_data, this_script)
	
		doc.usage(get_liquor_data , resource, startTime, None)	
		found = doc.entity('dat:aliyevaa_jgtsui#liquor_set', {prov.model.PROV_LABEL:'Liquor Stores', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_liquor_data, endTime)
		doc.wasDerivedFrom(found, resource, get_liquor_data, get_liquor_data, get_liquor_data)
		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()
		return doc



liquor_stores.execute()
doc = liquor_stores.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

