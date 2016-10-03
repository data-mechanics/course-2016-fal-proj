#import requrests
import urllib.request
import dml
import json
import pymongo
import prov.model
import uuid
import datetime


#from functools import reduce

from pymongo import MongoClient


class properties(dml.Algorithm):
	contributor = 'aliyevaa_jgtsui'
	reads = []
	writes = ['aliyevaa_jgtsui.lost', 'aliyevaa_jgtsui.found']
	
	@staticmethod
	def execute(trial = False):
		socrata_limit=5000
		entries=169199
		startTime = datetime.datetime.now()
		client = MongoClient()
		mrepo = client.repo
		mrepo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')
		mrepo.dropPermanent("property")
		mrepo.createPermanent("property")
		
		url ="https://data.cityofboston.gov/Permitting/Property-Assessment-2016/i7w8-ure5" 
		#data = urllib.request.urlopen(url).read().decode('utf8')
		#select = '&$select=pid,zipcode,av_total'
		select='&$select=ptype,pid,st_num,st_name,st_name_suf,zipcode,av_total,av_bldg,av_land,living_area'
		for i in range(0, 1 + entries // socrata_limit):
			limit  = '$limit='   + str(socrata_limit)
			offset = '&$offset=' + str(i * socrata_limit)
			query  = '?' + limit + offset + select
			result = urllib.request.urlopen(url + query).read().decode('utf-8')
			asmts  = json.loads(result)
			mrepo['aliyevaa_jgtsui.property'].insert_many(asmts)



		mrepo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}	
	
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):	
		return






properties.execute()

#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
