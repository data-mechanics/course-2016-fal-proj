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


class transformations(dml.Algorithm):
	contributor = 'aliyevaa_jgtsui'
	reads = ['aliyevaa_jgtsui.liquor_set','aliyevaa_jgtsui.bicycle_collisions','aliyevaa_jgtsui.crimeData', 'aliyevaa_jgtsui.liquor_data','aliyevaa_jgtsui.properties2016Data','aliyevaa_jgtsui.wazeAlertsData', 'aliyevaa_jgtsui.zip_codes_mapping']
	writes = ['aliyevaa_jgtsui.final']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = MongoClient()
		mrepo = client.repo
		mrepo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')
		
		mrepo.dropPermanent("final")
		mrepo.createPermanent("final")
		data_list=[]
		zipcodes=[]
		
		for x in mrepo.aliyevaa_jgtsui.zip_codes_mapping.find():
			y=dict(x)
			if (y['zipcode'])!='':
				n=int(y['zipcode'])
				zipcodes.append(n)
		count=0	
		flag=0
		for zipcode in zipcodes:
			for x in mrepo.aliyevaa_jgtsui.liquor_data.find():
				y=dict(x)	
				d={}
				if int(y['zip'])==zipcode:
					flag=1
					count=count+1
					
			if flag==1:
				d['number_of_liquor_stores']=count
				count=0
				d['zipcode']=zipcode
				data_list.append(d)
			flag=0
		prop_list=[]
		flag1=0
		for zipcode in zipcodes:
			for x in mrepo.aliyevaa_jgtsui.properties2016Data.find():
				y=dict(x)
				d={}
				try:
					k=int(y['zipcode'])
					if k==zipcode:
						d['prop_av_total']=float(y['av_total'])
						d['zipcode']=zipcode
						flag1=1
				except:
					pass
			if flag1==1:
				prop_list.append(d)
			flag1=0
		print(prop_list)							
		#print(data_list)	
		djson = json.loads(data_list)
		s=json.dumps(djson, sort_keys=True, indent=2)
		geolocator = Nominatim()

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



transformations.execute()
doc = transformations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
