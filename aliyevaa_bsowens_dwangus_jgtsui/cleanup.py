import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
from pymongo import MongoClient
from pymongo import ReadPreference
from pymongo import UpdateOne
import ast
from collections import OrderedDict

class cleanup(dml.Algorithm):
	contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
	reads = ['aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses']
	writes = ['aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses']
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = MongoClient()
		mrepo = client.repo
		mrepo.authenticate('aliyevaa_bsowens_dwangus_jgtsui', 'aliyevaa_bsowens_dwangus_jgtsui')
		#for item in mrepo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.find():
		#mrepo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.ensureIndex( { 'coordinates': 1 }, { unique: true, dropDups: true } )
		'''
		cursor = mrepo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.aggregate([{"$group": {"_id": "$ID", "unique_ids": {"$addToSet": "$_id"}, "count": {"$sum": 1}}},{"$match": {"count":{ "$gte": 2 }}}])
		response = []
		for doc in cursor:
			del doc["unique_ids"][0]
			for id in doc["unique_ids"]:
        			response.append(id)

		mrepo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.remove({"_id": {"$in": response}})
		for i in mrepo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.find():
			print(i)
		'''	
		lic=[]	
		count=0
		lis_list=[]
		for item in mrepo.aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses.find():					
			if item["licensedttm"] not in lis_list:
				lis_list.append( item['licenseno'])
				del item['_id']
				lic.append(item)		

		mrepo.dropPermanent("entertainment_licenses")
		mrepo.createPermanent("entertainment_licenses")
		#print(lic)
		str_l=', '.join(json.dumps(d) for d in lic)
		r=json.loads('['+str_l+']')		

	
		mrepo['aliyevaa_bsowens_dwangus_jgtsui.entertainment_licenses'].insert_many(r)

		
		mrepo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}	
	
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client =  dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(cleanup.contributor,cleanup.contributor)
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
		doc.add_namespace('dat', 'http://datamechanics.io/data/') 
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/') 
		doc.add_namespace('bdp', 'https://maps.googleapis.com/maps/api/place')

		this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#cleanup',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource =doc.entity('bdp:city=Boston', {'prov:label':'delete duplicates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)	
		doc.wasAssociatedWith(get_liquor_data, this_script)

		doc.usage(get_liquor_data , resource, startTime, None)	
		found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#cleanup', {prov.model.PROV_LABEL:'delete duplicates', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_liquor_data, endTime)
		doc.wasDerivedFrom(found, resource, get_liquor_data, get_liquor_data, get_liquor_data)
		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()
		return doc


		return

cleanup.execute()
doc = cleanup.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
