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


class example(dml.Algorithm):
	contributor = 'aliyevaa_jgtsui'
	reads = []
	writes = ['aliyevaa_jgtsui.lost', 'aliyevaa_jgtsui.found']
	
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = MongoClient()
		mrepo = client.repo
		mrepo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')
		
		mrepo.dropPermanent("liquor_data")
		mrepo.createPermanent("liquor_data")
		
		url = "https://data.cityofboston.gov/resource/hda6-fnsh.json?$limit=50000"
		data = urllib.request.urlopen(url).read().decode('utf8')
		djson = json.loads(data)
		s=json.dumps(djson, sort_keys=True, indent=2)
		geolocator = Nominatim()


		for entry in djson:
			location=entry['location']
			if location['latitude']=='0.0':
				djson.remove(entry)
			else:
				a=[]
				strLoc=location['latitude']+' , '+ location['longitude']
				location = geolocator.reverse(strLoc)
				addr=location.address
				a=addr.split(',')
				try:
					del entry ['location']
					entry['zip']=a[-2]
					#print(entry)
				except:
					djson.remove(entry)
					#if (isinstance(int(a[-2]), int))		
		
	
			
		repo['aliyevaa_jgtsui.liquor_data'].insert_many(djson)
		mrepo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}	
	
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):	
		return






example.execute()

#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

