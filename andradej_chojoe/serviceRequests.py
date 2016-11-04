
# coding: utf-8

# In[1]:

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

#set operations borrowed from lecture notes
def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]

def aggregate(R, f):
    results = []
    
    #keys are all possible unique zip codes
    keys_zipcodes = {r[0] for r in R}
    keys_types = {r[1] for r in R}
    
    total_requests = []
    
    for key_zip in keys_zipcodes:
        for key_type in keys_types:
            total_requests = []
            for value in R:
                if key_zip == value[0]:
                    if key_type == value[1]:
                        total_requests.append(1)
            results.append((key_zip, key_type, f(total_requests)))          
    return results 

def processData(row):
    try:
        if row['location_zipcode'] and row['type']:
            
            #need to convert zip code from int to zip format
            zipcode = "0" + str(row['location_zipcode'])
            desc = row['type']
            return(zipcode, desc)
    except:
        return None
    return None

#helper functions to filter the hotline requests that are related to sanitation
def sanitaryFilter(field):
    # if the field is None throw out of dataset
    if not field:
        return False
    
    #list of sanitary types in the mayor 24 hour hotline dataset
    sanitationFields = ['illegal dumping',                         'improper storage of trash (barrels)',                         'mice infestation - residential',                         'missed trash/recycling/yard waste/bulk item',                         'overflowing or un-kept dumpster',                         'pest infestation - residential',                         'rodent activity',                         'unsanitary conditions - establishment']

    if field[1].lower() in sanitationFields:
        return True
    else:
        return False
    
# removes values with counts as 0
def removeZeroOccurences(row):
    # if the count is 0 do not include in final set
    if row[2] <= 0:
        return False
    else:
        return True
    
#takes a list and translates its individual elements to dictionaries
def dictionarify(R):
    result = []
    for r in R:
        result.append((('zipcode', r[0]), ('type', r[1]), ('count', r[2])))
    return result


class serviceRequests(dml.Algorithm):
    contributor = 'andradej_chojoe'
    reads = []
    writes = ['andrade_chojoe.hotline']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        #Set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('andradej_chojoe', 'andradej_chojoe')
    
        repo.dropPermanent('andradej_chojoe.hotline_transf')
        repo.createPermanent('andradej_chojoe.hotline_transf')

        hotlineInfo = repo['andradej_chojoe.hotline'].find()
    
        #SAMPLES THE DATA
        #hotlineInfo = hotlineInfo[:500]

        hotlineInfo_filtered = project(hotlineInfo, processData)   
        hotlineInfo_filtered = select(hotlineInfo_filtered, sanitaryFilter)
        hotlineInfo_filtered = aggregate(hotlineInfo_filtered, sum)
        hotlineInfo_filtered = select(hotlineInfo_filtered, removeZeroOccurences)
        
        hotlineInfo_filtered = dictionarify(hotlineInfo_filtered)

        for t in hotlineInfo_filtered:
            t = dict(t)
            repo['andradej_chojoe.hotline_transf'].insert_one(t)
            
        endTime = datetime.datetime.now()
        return{'start':startTime, 'end':endTime}

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
        
        this_script = doc.agent('alg:andradej_chojoe#serviceRequests', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        service_resource = doc.entity('bdp:jbcd-dknd', {'prov:label':'24 Hour Mayor Hotline',                                                 prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_service = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_service, this_script)
        
        doc.usage(get_service, service_resource, startTime, None,                   {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        service = doc.entity('dat:andradej_chojoe#bigbelly', {prov.model.PROV_LABEL:'Service Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(service, this_script)
        doc.wasGeneratedBy(service, get_service, endTime)
        doc.wasDerivedFrom(service, service_resource, get_service, get_service, get_service)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc
    
serviceRequests.execute()
doc = serviceRequests.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


# In[ ]:



