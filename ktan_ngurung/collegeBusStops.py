import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geocoder
import re

class example(dml.Algorithm):
    contributor = 'ktan_ngurung'
    reads = ['ktan_ngurung.colleges', 'ktan_ngurung.busStops']
    writes = ['ktan_ngurung.collegeBusStopCounts']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')

        # Get bus stop and college location data
        busStops = repo.ktan_ngurung.busStops.find() 
        colleges = repo.ktan_ngurung.colleges.find()
        collegeAndStopCountsDict = {}
        
        for stop in busStops:

            coordinates = stop['fields']['geo_point_2d']
            address = geocoder.google(coordinates, method='reverse')
            zipcode = str(address.postal)

            if zipcode != 'None' and zipcode not in collegeAndStopCountsDict:
                collegeAndStopCountsDict[zipcode] = {'busStopCount' : 1, 'collegeCount' : 0}

            elif zipcode != 'None' and zipcode in collegeAndStopCountsDict:
                collegeAndStopCountsDict[zipcode]['busStopCount'] += 1

            else:
                pass

        for college in colleges:

            coordinates = stop['fields']['geo_point_2d']
            address = geocoder.google(coordinates, method='reverse')
            zipcode = str(address.postal)

            if zipcode != 'None' and zipcode in collegeAndStopCountsDict:
                collegeAndStopCountsDict[zipcode]['collegeCount'] += 1

            elif zipcode != 'None' and zipcode not in collegeAndStopCountsDict:
                collegeAndStopCountsDict[zipcode] = {'busStopCount' : 0, 'collegeCount': 1}

            else:
                pass

        print(collegeAndStopCountsDict)

    @staticmethod			
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    	pass

example.execute() 
doc = example.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

