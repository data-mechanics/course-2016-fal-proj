import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
import ast

class retrieveData(dml.Algorithm):
    contributor = 'dwangus'
    reads = []
    
    titles = ['Crime Incident Reports (July 2012 - August 2015) (Source: Legacy System)', \
              'Public Access Fishing Locations', 'Issued Moving Truck Permits', 'Active Food Establishment Licenses', \
              'Entertainment Licenses', 'Community Supported Agriculture (CSA) Pickups ', 'Year-Round Swimming Pools']
    setExtensions = ['crime2012-2015', 'public-fishing-access-locations', 'moving-truck-permits', \
                     'food-licenses', 'entertainment-licenses', 'csa-pickups', 'year-round-pools']

    writes = ['dwangus.' + dataSet for dataSet in setExtensions]

    urls = ['https://data.cityofboston.gov/resource/ufcx-3fdn.json', \
            'https://data.cityofboston.gov/resource/jaz3-9yrd.json', \
            'https://data.cityofboston.gov/resource/bzif-fkwd.json', \
            'https://data.cityofboston.gov/resource/fdxy-gydq.json', \
            'https://data.cityofboston.gov/resource/cz6t-w69j.json', \
            'https://data.cityofboston.gov/resource/cqit-55tt.json', \
            'https://data.cityofboston.gov/resource/5jxx-wfpr.json']

    dataSetDict = {}
    for i in range(len(setExtensions)):
        dataSetDict[setExtensions[i]] = (urls[i], writes[i], titles[i], urls[i][39:48])
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()
        start = time.time()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(retrieveData.contributor, retrieveData.contributor)
        myrepo = repo.dwangus

        '''
        ###Notes for each of the datasets' geo-location data and format###
         # crime2012-2015                       is fine
         # public-fishing-access-locations      requires 'map_location' to be renamed to 'location', and its
         #                                          'location' to be renamed to something else
         #                                          // Let's say rename to 'location_address'
         # moving-truck-permits                 requires 'location' to be renamed to something else, and
         #                                          its location.longitude and location.latitude to be put
         #                                          into proper MongoDB GeoJSON object-format
         #                                          // Let's say rename to 'location_details'
         # food-licenses                        is fine
         # entertainment-licenses               requires its 'location': '(lat., long.)' format to be put
         #                                          into propert MongoDB GeoJSON object-format (and switch
         #                                          order to (long, lat))
         # csa-pickups                          requires 'map_location' to be renamed to 'location', and its
         #                                          'location' to be renamed to something else
         #                                          // Let's say rename to 'location_address'
         # **************************************************************************************************************
         # year-round-pools                     ...we need to figure out what this is --> 'coordinates': [2952740, 754243]
         #      Look into UTM coordinates?
         #      Google "large x coordinate in what format to latitude"
        '''
        for key in retrieveData.dataSetDict.keys():
            print("Starting retrieval and storage of {} dataset".format(key))
            repo.dropPermanent(key)
            repo.createPermanent(key)
            response = urllib.request.urlopen(retrieveData.dataSetDict[key][0]).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            repo[retrieveData.dataSetDict[key][1]].insert_many(r)
            ###TRANSFORMATION###
            if key == 'public-fishing-access-locations':
                print("Transforming public-fishing-access-locations dataset...")
                fishing = myrepo['public-fishing-access-locations']
                fishing.update_many({}, {"$rename": {'location': 'location_address'}})
                fishing.update_many({}, {"$rename": {'map_location': 'location'}})
                fishing.create_index([('location', '2dsphere')])
            elif key == 'csa-pickups':
                print("Transforming csa-pickups dataset...")
                csa = myrepo['csa-pickups']
                csa.update_many({}, {"$rename": {'location': 'location_address'}})
                csa.update_many({}, {"$rename": {'map_location': 'location'}})
                csa.create_index([('location', '2dsphere')])
            elif key == 'food-licenses':
                print("Transforming food-licenses dataset...")
                food = myrepo['food-licenses']
                food.create_index([('location', '2dsphere')])
            elif key == 'entertainment-licenses':
                print("Transforming entertainment-licenses dataset...")
                ent = myrepo['entertainment-licenses']
                for e in ent.find(modifiers={"$snapshot": True}):
                    if 'location' in e.keys() and type(e['location']) == str and e['location'].startswith('('):
                        prevCoords = ast.literal_eval(e['location'])
                        ent.update({'_id': e['_id']}, \
                                    {'$set': {'location': {'type': 'Point', 'coordinates': [prevCoords[1], prevCoords[0]]}}})
                    else:
                        ent.delete_one({'_id': e['_id']})
                ent.create_index([('location', '2dsphere')])
            elif key == 'year-round-pools':
                #Transform year-round-pools geolocation field format
                #Putting this off for now... still don't know what its current format is...
                continue
            elif key == 'moving-truck-permits':
                print("Transforming moving-truck-permits dataset...")
                truck = myrepo['moving-truck-permits']
                truck.update_many({}, {"$rename": {'location': 'location_details'}})
                for t in truck.find(modifiers={"$snapshot": True}):
                    if 'location_details' in t.keys():
                        prevCoords = [float(t['location_details']['longitude']), float(t['location_details']['latitude'])]
                        truck.update({'_id': t['_id']}, \
                                   {'$set': {'location': {'type': 'Point', 'coordinates': prevCoords}}})
                truck.create_index([('location', '2dsphere')])

        repo.logout()

        endTime = datetime.datetime.now()
        print("\nRetrieval and storage of datasets took:\n{} seconds\n...and ended at:\n{}\n".format(time.time() - start, endTime))
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(retrieveData.contributor, retrieveData.contributor)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:dwangus#retrieveData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        for key in retrieveData.dataSetDict.keys():
            resource = doc.entity('bdp:' + retrieveData.dataSetDict[key][3], {'prov:label':retrieveData.dataSetDict[key][2], prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
            get_something = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(get_something, this_script)
            something = doc.entity('dat:dwangus#' + key, {prov.model.PROV_LABEL:retrieveData.dataSetDict[key][2], prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(something, this_script)
            doc.wasGeneratedBy(something, get_something, endTime)
            doc.wasDerivedFrom(something, resource, get_something, get_something, get_something)

        '''
        doc.usage(get_found, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                }
            )
        doc.usage(get_lost, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                }
            )
        #'''

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

retrieveData.execute()
doc = retrieveData.provenance()
#print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
