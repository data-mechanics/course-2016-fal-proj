import json
import dml
import prov.model
import datetime
import uuid
import time
import ast
from geopy.distance import vincenty as vct
from bson.code import Code

class scoreLocations(dml.Algorithm):
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'

    oldSetExtensions = ['crime2012_2015', 'public_fishing_access_locations', 'moving_truck_permits', \
                     'food_licenses', 'entertainment_licenses', 'csa_pickups', 'year_round_pools','parking', \
                     'libraries']
    oldTitles = ['Crime Incident Reports (July 2012 - August 2015) (Source: Legacy System)', \
              'Public Access Fishing Locations', 'Issued Moving Truck Permits', 'Active Food Establishment Licenses', \
              'Entertainment Licenses', 'Community Supported Agriculture (CSA) Pickups ', 'Year-Round Swimming Pools', \
              'Parking Lots', 'Public Libraries']
    
    titles = ['Community Indicators Location and Score']
    setExtensions = ['community_indicators']

    reads = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in oldSetExtensions]
    writes = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in setExtensions]

    dataSetDict = {}
    for i in range(len(setExtensions)):
        dataSetDict[setExtensions[i]] = (writes[i], titles[i])
    print(dataSetDict)

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()
        print("Starting execution of script @{}".format(startTime))
        start = time.time()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(scoreLocations.contributor, scoreLocations.contributor)
        myrepo = repo.aliyevaa_bsowens_dwangus_jgtsui
            
        newName = 'community_indicators'
        indicatorsColl = myrepo[newName]
        repo.dropPermanent(newName)
        repo.createPermanent(newName)

        pos_count = 0
        neg_count = 0

        for key in scoreLocations.oldSetExtensions:
            begin = time.time()

            newSet = myrepo[key]

            communityIndicators = ['public_fishing_access_locations','csa_pickups','year_round_pools','libraries']
            anti_communityIndicators = ['food_licenses', 'entertainment_licenses','parking']

            print("Generating from old {} dataset...".format(key))
            if key in communityIndicators:
                i = 0
                for doc in newSet.find(modifiers={"$snapshot": True}):
                    if (i%100 == 0):
                        print(i)
                    i += 1
                    if 'location' in doc.keys():

                        #get the title of the business, based on common title keys

                        if key == 'public_fishing_access_locations':
                            title = doc['name']
                        elif key == 'csa_pickups':
                            title = doc['name']
                        elif key == 'year_round_pools':
                            title = doc['business_name']
                        elif key == 'libraries':
                            title = doc['name']
                        else: title = "unknownName " + i

                        pos_count += 1
                        indicatorsColl.insert({'title': title, 'type':key,
                                             'location': doc['location'],
                                            'community_score': 1})

            elif key in anti_communityIndicators:
                i = 0
                for doc in newSet.find(modifiers={"$snapshot": True}):
                    if (i % 100 == 0):
                        print(i)
                    i += 1
                    if 'location' in doc.keys():
                        # get the title of the business, based on common title keys
                        if key == 'entertainment_licenses':
                            title = doc['dbaname']
                        elif key == 'food_licenses':
                            title = doc['businessname']
                        elif key == 'parking':
                            title = doc['name']
                        else: title = 'unknownName ' + i

                        neg_count += 1
                        indicatorsColl.insert({'id': doc['_id'], 'title': title, 'type': key,
                                                    'location': doc['location'],
                                               'community_score': -1})
            print("Processing {} took {} seconds.\n".format(key, time.time() - begin))

        repo.logout()

        endTime = datetime.datetime.now()
        print("\nTransformation/manipulation of old + storage of new datasets took:\n{} seconds\n...and ended at:\n{}\n".format(time.time() - start, endTime))
        #"Transformation/manipulation of old + storage of new datasets took:
        #3675.0184156894684 seconds
        #...and ended at:
        #2016-10-07 06:58:25.821880"
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
        repo.authenticate(scoreLocations.contributor, scoreLocations.contributor)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#scoreLocations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        for key in scoreLocations.dataSetDict.keys():
            #How to say that this dataset was generated from multiple sources?
            #resource = doc.entity('dat:' + transformOldAggregateNew.contributor + '#' + key???, {'prov:label':transformOldAggregateNew.dataSetDict[key][1], prov.model.PROV_TYPE:'ont:DataSet'})
            get_something = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(get_something, this_script)
            something = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#' + key, {prov.model.PROV_LABEL:scoreLocations.dataSetDict[key][1], prov.model.PROV_TYPE: 'ont:DataSet'})
            doc.wasAttributedTo(something, this_script)
            doc.wasGeneratedBy(something, get_something, endTime)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

scoreLocations.execute()
doc = scoreLocations.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
