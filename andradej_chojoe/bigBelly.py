
# coding: utf-8

# In[2]:

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
import math
import sys

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]

# Takes in a key value set and applies function to values of the set.
# In this scenario, average fullness percentage is found and the total number of bellies
# per zipcode are found
# @R: set of key, value pairs
# @f: function applied to the value of the pair
def aggregate(R, f):
    
    fullness = []
    count = []
    results = []
    
    #keys are all possible unique zip codes
    keys = {r[0] for r in R}
    
    for key in keys:
        fullness = [] #reset fullness tracker
        count = [] #reset count tracker
        for value in R:
            if key == value[0]: 
                fullness.append(value[1])
                count.append(value[2])
        results.append((key, float(f(fullness)/f(count)), f(count)))
    
    return results #:D

# Processes and projects the current data in the dataset to produce key, value pair
# Key: Zipcode
# Value: percentage full, count
# @dict: gets raw data from city of boston data portal
def processData(row):
    lat = row['location']['coordinates'][1]
    lon = row['location']['coordinates'][0]
    percentage = fullnessPercentage(row['fullness'])

    #we append the newly contructed tuple format while appending a 1 to each tuple for 
    #summation purposes later
    return (((lat, lon), percentage, 1))

# Takes in a string representation of color and returns the associated fullness percentage              
# @color: String of color (ex: Red, Yellow, Green)
def fullnessPercentage(color):
    if color == 'GREEN':
        return 0.2
    elif color == 'YELLOW':
        return 0.6
    elif color == 'RED':
        return 1.0
    else:
        return 0
    
#takes a list and translates its individual elements to dictionaries
def dictionarify(R):
    result = []
    for r in R:
        #result.update('zipcode': r[0], 'days': r[1]})
        result.append((('coordinates', r[0]), ('percentage', r[1]), ('count', r[2])))
    return result

# -----------------------Geolocation functions (START - fix)----------------------------------------
def retrieveLocations(row):
    try:
        if row['geocoded_location']['coordinates'] and row['p_zipcode']:
            zipcode = row['p_zipcode']
            lat = row['geocoded_location']['coordinates'][1]
            lon = row['geocoded_location']['coordinates'][0]
            coordinates = (lat, lon)
            return (zipcode, coordinates)      
    except:
        return None
    return None

# does cleanup on the projection to get rid of None values
def removeNoneValues(row):
    if row:
        return True
    else:
        return False

#finds all associated coordinates with a zipcode
def aggregateAllCoordinates(R):
    #keys are all possible unique zip codes
    keys = {r[0] for r in R}
    
    # contains all associated coordinates
    coordinates = []
    
    # contains the resulting set
    result = []
    
    for key in keys:
        coordinates = []
        for value in R:
            if value[0] == key:
                coordinates.append(value[1])
        result.append((key, list(set(coordinates))))
    return result

# returns a centroid for each zipcode - intended to be used as a k means clustering centroid by zipcode
def findRegions(row):
    x_coor = []
    y_coor = []
    for coordinate in row[1]:
        x_coor.append(coordinate[0])
        y_coor.append(coordinate[1])
    centroid_x = sum(x_coor) / len(x_coor)
    centroid_y = sum(y_coor) / len(y_coor)                   
    return (row[0], (centroid_x, centroid_y))

# special projection transformation to convert geolocations to zipcode
def convert(R, S):
    return [convertCoordinatesToZip(r, S) for r in R]

def convertCoordinatesToZip(row, zip_coor):
    min_dist = float('inf')
    zipcode = ""
    for value in zip_coor:
        if euclideanDistance(value[1], row[0]) < min_dist:
            min_dist = euclideanDistance(value[1], row[0])
            zipcode = value[0]
    return (zipcode, row[1], row[2])
    
def euclideanDistance(coor1,coor2):
    return math.sqrt((coor2[0] - coor1[0])**2 + (coor2[1] - coor1[0])**2)
# -----------------------Geolocation functions (END - fix)----------------------------------------


class bigbelly(dml.Algorithm):
    contributor = 'andradej_chojoe'
    reads = []
    writes = ['andrade_chojoe.bigbelly']

# -------------------- Fix this ---------------------------------------------
#     #Grabs Master Address List to figure out zipcodes based on Geolocation
#     addrCoordinates = "https://data.cityofboston.gov/resource/je5q-tbjf.json"
#     geoCoordinates = urllib.request.urlopen(addrCoordinates).read().decode("utf-8")
#     geoCoordinates = json.dumps(json.loads(geoCoordinates), sort_keys=True, indent=2)
#     geoCoordinates = json.loads(geoCoordinates) #converts to list and dict
    
#     geolocCoor = project(geoCoordinates, retrieveLocations)
#     geolocCoor = select(geolocCoor, removeNoneValues)
#     geolocCoor = aggregateAllCoordinates(geolocCoor)
#     geolocCoor = project(geolocCoor, findRegions)
#     print(geolocCoor) #zipcode with centriod coordinate
# -------------------- Fix this ---------------------------------------------

# -------------------- Fix this --------------------------------------------
#     get big belly info from repo
#     # use other dataset to get big belly zipcodes
#     bigbelly_filtered = convert(bigbelly_filtered, geolocCoor)
#     bigbelly_filtered = aggregate(bigbelly_filtered, sum)
#     print(bigbelly_filtered)
# -------------------- Fix this --------------------------------------------
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        #Set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('andradej_chojoe', 'andradej_chojoe')
        
        repo.dropPermanent('andradej_chojoe.bigbelly_transf')
        repo.createPermanent('andradej_chojoe.bigbelly_transf')

        bigbellyinfo = repo['andradej_chojoe.bigbelly'].find()
    
        #samples data
        #bigbellyinfo = bigbellyinfo[:100]
    
        # transformations
        bigbelly_filtered = project(bigbellyinfo, processData)
        bigbelly_filtered = dictionarify(bigbelly_filtered)
        
        
        for t in bigbelly_filtered:
            t = dict(t)
            repo['andradej_chojoe.bigbelly_transf'].insert_one(t)

        endTime = datetime.datetime.now()
        return{'start':startTime, "end":endTime} 
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('andradej_chojoe', 'andradej_chojoe')

        doc = prov.model.ProvDocument()
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/andradej_chojoe/') # The scripts in / format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/andradej_chojoe/') # The data sets in / format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        
        this_script = doc.agent('alg:andradej_chojoe#bigBelly', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        bigbelly_rsc = doc.entity('bdp:nybq-xu5r', {'prov:label':'Big Belly Alerts 2014',                                                 prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bigbelly = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bigbelly, this_script)
        
        doc.usage(get_bigbelly, bigbelly_rsc, startTime, None,                   {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        bigbelly = doc.entity('dat:andradej_chojoe#bigbelly', {prov.model.PROV_LABEL:'Big Belly Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(bigbelly, this_script)
        doc.wasGeneratedBy(bigbelly, get_bigbelly, endTime)
        doc.wasDerivedFrom(bigbelly, bigbelly_rsc, get_bigbelly, get_bigbelly, get_bigbelly)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

bigbelly.execute()
doc = bigbelly.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


# In[ ]:



