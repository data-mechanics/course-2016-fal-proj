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

    #oldSetExtensions = ['crime2012_2015', 'public_fishing_access_locations', 'moving_truck_permits', \
    #                 'food_licenses', 'entertainment_licenses', 'csa_pickups', 'year_round_pools','parking', \
    #                 'libraries']
    #oldTitles = ['Crime Incident Reports (July 2012 - August 2015) (Source: Legacy System)', \
    #          'Public Access Fishing Locations', 'Issued Moving Truck Permits', 'Active Food Establishment Licenses', \
    #          'Entertainment Licenses', 'Community Supported Agriculture (CSA) Pickups ', 'Year-Round Swimming Pools', \
    #          'Parking Lots', 'Public Libraries']
    
    #Forgot to include 'entertainment_licenses_no_restaurants' dataset -- 'Distinct Entertainment Licenses (without restaurants)' title
    oldSetExtensions = ['crime2012_2015', 'public_fishing_access_locations', 'moving_truck_permits', \
                     'food_licenses', 'entertainment_licenses_no_restaurants', 'csa_pickups', 'year_round_pools','parking', \
                     'libraries']
    oldTitles = ['Crime Incident Reports (July 2012 - August 2015) (Source: Legacy System)', \
                 'Public Access Fishing Locations', 'Issued Moving Truck Permits', 'Active Food Establishment Licenses', \
                 'Distinct Entertainment Licenses (without restaurants)', 'Community Supported Agriculture (CSA) Pickups ', \
                 'Year-Round Swimming Pools', 'Parking Lots', 'Public Libraries']
    
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
        
        #communityIndicators = ['public_fishing_access_locations','csa_pickups','year_round_pools','libraries']#No year round pools!
        communityIndicators = ['public_fishing_access_locations','csa_pickups','libraries']
        anti_communityIndicators = ['food_licenses', 'entertainment_licenses_no_restaurants','parking']

        pos_count = 0
        neg_count = 0
        #ratio = 198/1858 # ratio is pos/neg counts # --> Man, don't hardcode values!
        
        for key in scoreLocations.oldSetExtensions:
            print("Counting {} dataset.".format(key))
            tempBefore = pos_count + neg_count
            countingSet = myrepo[key]
            if key in communityIndicators:
                for doc in countingSet.find(modifiers={"$snapshot": True}):
                    if 'location' in doc.keys():
                        pos_count += 1
            elif key in anti_communityIndicators:
                for doc in countingSet.find(modifiers={"$snapshot": True}):
                    if 'location' in doc.keys():
                        neg_count += 1
            print("Difference in counted entries vs. size of {} dataset: {} vs. {}".format(key, pos_count+neg_count-tempBefore, countingSet.count()))
        
        posRatio = neg_count/pos_count if pos_count > neg_count else 1
        negRatio = pos_count/neg_count if neg_count > pos_count else 1
        print("Finished counting all positive and negative indicators: + {}, - {}".format(pos_count, neg_count))
        print("Positive factor-weight: {}, Negative factor-weight: {}".format(posRatio, negRatio))
        
        for key in scoreLocations.oldSetExtensions:
            begin = time.time()
            newSet = myrepo[key]

            print("Generating from old {} dataset...".format(key))
            if key in communityIndicators:
                i = 0
                for doc in newSet.find(modifiers={"$snapshot": True}):
                    if (i%100 == 0) and i > 0:
                        print(i)
                    i += 1
                    if 'location' in doc.keys():

                        #get the title of the business, based on common title keys
                        if key == 'public_fishing_access_locations':
                            title = doc['name']
                        elif key == 'csa_pickups':
                            title = doc['name']
                        #elif key == 'year_round_pools':
                        #    title = doc['business_name']
                        elif key == 'libraries':
                            title = doc['name']
                        else: title = "unknownName " + i

                        indicatorsColl.insert({'title': title, 'type':key,
                                             'location': doc['location'],
                                            'community_score': 1*posRatio})

            elif key in anti_communityIndicators:
                i = 0
                for doc in newSet.find(modifiers={"$snapshot": True}):
                    if (i % 100 == 0) and i > 0:
                        print(i)
                    i += 1
                    if 'location' in doc.keys():
                        # get the title of the business, based on common title keys
                        if key == 'entertainment_licenses_no_restaurants':
                        #if key == 'entertainment_licenses':
                            title = doc['dbaname']
                        elif key == 'food_licenses':
                            title = doc['businessname']
                        elif key == 'parking':
                            title = doc['name']
                        else: title = 'unknownName ' + i

                        # note: s
                        indicatorsColl.insert({'id': doc['_id'], 'title': title, 'type': key,
                                                    'location': doc['location'],
                                               'community_score': -1*negRatio})
                        
            print("Processing {} took {} seconds.\n".format(key, time.time() - begin))

        repo.logout()
        #print("pos count =", pos_count) = 198
        #print("neg count =", neg_count) = 1858
        # ratio =
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

#scoreLocations.execute()
#doc = scoreLocations.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))

def main():
    print("Executing: scoreLocations.py")
    scoreLocations.execute()
    doc = scoreLocations.provenance()

## eof
