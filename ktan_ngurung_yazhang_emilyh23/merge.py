import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geocoder
from collections import Counter
import pandas as pd

class merge(dml.Algorithm):
    contributor = 'ktan_ngurung_yazhang_emilyh23'
    reads = ['ktan_ngurung_yazhang_emilyh23.colleges', 'ktan_ngurung_yazhang_emilyh23.busStops']
    writes = ['ktan_ngurung_yazhang_emilyh23.collegeBusStopCounts']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')

        # Get bus stop and college location data
        hbbCounts = repo.ktan_ngurung_yazhang_emilyh23.hubwayBigBellyCounts.find_one() 
        cbsCounts = repo.ktan_ngurung_yazhang_emilyh23.collegeBusStopCounts.find_one()
        tRideCounts = repo.ktan_ngurung_yazhang_emilyh23.tRidershipLocation.find_one()

        hbbCbsDict = {} 
        tRideTransformed = []
        #NISA CODE  




        #KRISTEL_YAO CODE ew

        # Transformed tRideCounts to dataframe
        for s in tRideCounts:
            try:
                temp = {'zc': tRideCounts[s]['zipcode'], 'loc':s, 'entry':tRideCounts[s]['entries']}
                tRideTransformed.append(temp)
            except TypeError:
                pass
        #print(tRideTransformed)
        df = pd.DataFrame(tRideTransformed)
        print(df)
       
        # Convert dictionary into JSON object 
        # data = json.dumps(collegeAndStopCountsDict, sort_keys=True, indent=2)
        # r = json.loads(data)

        # # Create new dataset called tRidershipLocation
        # repo.dropPermanent("collegeBusStopCounts")
        # repo.createPermanent("collegeBusStopCounts")
        # repo['ktan_ngurung_yazhang_emilyh23.collegeBusStopCounts'].insert_one(r)

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
        repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:ktan_ngurung_yazhang_emilyh23#collegeBusStops', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        colleges_resource = doc.entity('dat:ktan_ngurung_yazhang_emilyh23/colleges-and-universities', {'prov:label':'Colleges and Universities', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        busStops_resource = doc.entity('dat:ktan_ngurung_yazhang_emilyh23/mbta-bus-stops', {'prov:label':'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, colleges_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )
        doc.usage(this_run, busStops_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        collegeBusStops = doc.entity('dat:ktan_ngurung_yazhang_emilyh23#collegeBusStops', {prov.model.PROV_LABEL:'Number of Colleges And Bus Stops for Each Zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(collegeBusStops, this_script)
        doc.wasGeneratedBy(collegeBusStops, this_run, endTime)
        doc.wasDerivedFrom(collegeBusStops, colleges_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(collegeBusStops, busStops_resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

merge.execute() 
#doc = collegeBusStops.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof