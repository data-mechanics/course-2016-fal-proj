import dml
import json
import pymongo
import prov.model
import uuid
import datetime
from bs4 import BeautifulSoup
import requests
from collections import OrderedDict
from pymongo import MongoClient


class zip_codes_mapping(dml.Algorithm):
	contributor = 'aliyevaa_jgtsui'
	reads = []
	writes = ['aliyevaa_jgtsui.zip_codes_mapping']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = MongoClient()
		mrepo = client.repo
		mrepo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')
		
		mrepo.dropPermanent("zip_codes_mapping")
		mrepo.createPermanent("zip_codes_mapping")

		url='http://boston.areaconnect.com/zip2.htm?city=Boston'
		source_code=requests.get(url)
		plain_text=source_code.text
		soup=BeautifulSoup(plain_text)
		rows=soup.findAll( 'div', {'class' : 'row'} )

		zipcodes_list=[]
		for zipcode in rows:
			zip_s = OrderedDict()
			zip_n_tag=str(zipcode.find('div', {'class':'block zip1'}))
			zip_s['zipcode']=zip_n_tag[27:32]
			zip_lat=str(zipcode.find('div', {'class':'block zip6'}))
			zip_s['latitude']=zip_lat[24:30]
			zip_lon=str(zipcode.find('div', {'class':'block zip7'}))
			zip_s['longitude']=zip_lon[24:31]
			zipcodes_list.append(zip_s)

		s=json.dumps(zipcodes_list, sort_keys=True, indent=2)
		mrepo['aliyevaa_jgtsui.zip_codes_mapping'].insert_many(zipcodes_list)
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
		doc.add_namespace('bdp', 'http://boston.areaconnect.com/zip2.htm?city=Boston')

		this_script = doc.agent('alg:aliyevaa_jgtsui#zip_codes_mapping',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource =doc.entity('bdp:city=Boston', {'prov:label':'Zip Codes to Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)	
		doc.wasAssociatedWith(get_liquor_data, this_script)

		doc.usage(get_liquor_data , resource, startTime, None)	
		found = doc.entity('dat:aliyevaa_jgtsui#zip_codes_mapping', {prov.model.PROV_LABEL:'Zip Codes to Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_liquor_data, endTime)
		doc.wasDerivedFrom(found, resource, get_liquor_data, get_liquor_data, get_liquor_data)
		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()
		return doc


zip_codes_mapping.execute()
doc = zip_codes_mapping.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))




