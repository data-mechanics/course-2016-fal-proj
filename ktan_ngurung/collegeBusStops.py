from geopy.geocoders import Nominatim
import re 
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

geolocator = Nominatim()

class collegeBusStops(dml.Algorithm):
    contributor = 'ktan_ngurung'
    reads = ['ktan_ngurung.colleges', 'ktan_ngurung.busStops']
    writes = ['ktan_ngurung.collegeBusStopCounts']
    
    @staticmethod
    def execute(trial = False): 
    	startTime = datetime.datetime.now()

    	client = dml.pymongo.MongoClient()
    	repo = client.repo
    	repo.authenticate('ktan_ngurung', 'ktan_ngurung')

    	busStops = repo.ktan_ngurung.busStops.find() 
    	colleges = repo.ktan_ngurung.colleges.find()

    	collegeAndStopCountsDict = {}
    	for stop in busStops:
            coordinate = stop['fields']['geo_point_2d']
            location = geolocator.reverse(str(coordinate[0]) + ' , ' + str(coordinate[1]))
            address = location.address
            reg = re.compile('^.*(?P<zipcode>\d{5}).*$')
            match = reg.match(address)
            if match:
                zipcode = match.groupdict()['zipcode']
            if zipcode not in busDict:
                collegeAndStopCountsDict[zipcode]['busStopCount'] = 1
                collegeAndStopCountsDict[zipcode]['collegeCount'] = 0
            else:
                collegeAndStopCountsDict[zipcode]['count'] += 1
            
    	for college in colleges:
            collegeZipCode = college['fields']['address'][-5:]
            if collegeZipCode in collegeAndStopCountsDict:
                collegeAndStopCountsDict[collegeZipCode]['collegeCount'] += 1
        print(collegeAndStopCountsDict)
        
    @staticmethod			
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    	pass

collegeBusStops.execute() 
