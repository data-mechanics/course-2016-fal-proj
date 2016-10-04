import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geocoder
from collections import Counter

class hubwayBigBelly(dml.Algorithm):
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
        hubway = repo.ktan_ngurung.hubway.find() 
        bigBelly = repo.ktan_ngurung.bigBelly.find()
        hbbDict = {}

        # Commented block below can only be a when query limit for geocoder library has not been used up
        
        for item in bigBelly:
            print(item['data'][1])
        #     address = geocoder.google(coordinates, method='reverse')
        #     zipcode = str(address.postal)

        #     if zipcode != 'None' and zipcode not in collegeAndStopCountsDict:
        #         collegeAndStopCountsDict[zipcode] = {'busStopCount' : 1, 'collegeCount' : 0}

        #     elif zipcode != 'None' and zipcode in collegeAndStopCountsDict:
        #         collegeAndStopCountsDict[zipcode]['busStopCount'] += 1

        #     else:
        #         pass

        # for college in colleges:

        #     coordinates = college['fields']['geo_point_2d']
        #     address = geocoder.google(coordinates, method='reverse')
        #     zipcode = str(address.postal)

        #     if zipcode != 'None' and zipcode in collegeAndStopCountsDict:
        #         collegeAndStopCountsDict[zipcode]['collegeCount'] += 1

        #     elif zipcode != 'None' and zipcode not in collegeAndStopCountsDict:
        #         collegeAndStopCountsDict[zipcode] = {'busStopCount' : 0, 'collegeCount': 1}

        #     else:
        #         pass
        
  
        # Convert dictionary into JSON object 
        # data = json.dumps(collegeAndStopCountsDict, sort_keys=True, indent=2)
        # r = json.loads(data)

        # # Create new dataset called tRidershipLocation
        # repo.dropPermanent("collegeBusStopCounts")
        # repo.createPermanent("collegeBusStopCounts")
        # repo['ktan_ngurung.collegeBusStopCounts'].insert_one(r)

    @staticmethod           
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # '''
        # Create the provenance document describing everything happening
        # in this script. Each run of the script will generate a new
        # document describing that invocation event.
        # '''

        # # Set up the database connection.
        # client = dml.pymongo.MongoClient()
        # repo = client.repo
        # repo.authenticate('ktan_ngurung', 'ktan_ngurung')
        
        # doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        # doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        # doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        # doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        # doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        # doc.add_namespace('ods', 'https://boston.opendatasoft.com/api/records/1.0/search/?dataset=') 

        # this_script = doc.agent('alg:ktan_ngurung#collegeBusStops', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        # colleges_resource = doc.entity('ods:colleges-and-universities', {'prov:label':'Colleges and Universities', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        # busStops_resource = doc.entity('ods:mbta-bus-stops&facet=town', {'prov:label':'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        # this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        # doc.wasAssociatedWith(this_run, this_script)

        # doc.usage(this_run, colleges_resource, startTime, None,
        #         {prov.model.PROV_TYPE:'ont:Retrieval'}
        #     )
        # doc.usage(this_run, busStops_resource, startTime, None,
        #         {prov.model.PROV_TYPE:'ont:Retrieval'}
        #     )

        # collegeBusStops = doc.entity('dat:ktan_ngurung#collegeBusStops', {prov.model.PROV_LABEL:'Number of Colleges And Bus Stops for Each Zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        # doc.wasAttributedTo(collegeBusStops, this_script)
        # doc.wasGeneratedBy(collegeBusStops, this_run, endTime)
        # doc.wasDerivedFrom(collegeBusStops, colleges_resource, this_run, this_run, this_run)
        # doc.wasDerivedFrom(collegeBusStops, busStops_resource, this_run, this_run, this_run)

        # repo.record(doc.serialize()) # Record the provenance document.
        # repo.logout()

        # return doc
        pass 

hubwayBigBelly.execute() 