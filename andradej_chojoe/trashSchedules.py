
# coding: utf-8

# In[8]:

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

#custom aggregate function :)
def aggregate(R, f):
    days = []
    results = []
    
    #keys are all possible unique location coordinates
    keys = [r[0] for r in R]
    
    for key in keys:
        for value in R:
            if key == value[0]: 
                if len(value[1]) > 1:
                    for day in value[1]:
                        days.append(day)
                else:
                    days.append(value[1]) 
        results.append((key, list(f(days)))) #first get rid of duplicates, then return the final list 
    
    return results 

def processData(row):
    try:
        if row['trash_day'] and row['p_zipcode']:
            trashDay = row['trash_day']
            trashLoc = row['p_zipcode']
            return (trashLoc, trashDay)
    except:
        return None
    return None

def removeNoneValues(row):
    if not row:
        return False
    else:
        return True
    
#takes a list and translates its individual elements to dictionaries
def dictionarify(R):
    result = []
    for r in R:
        result.append((('zipcode', r[0]), ('days', r[1])))
    return result


#note: retrieving the entire raw data set is extremely slow
#to see functionality, we recommend sampling the data aka.
#(uncommenting the line that samples trashSchInfo)
class trashSchedules(dml.Algorithm):
    contributor = 'andradej_chojoe'
    reads = []
    writes = ['andrade_chojoe.bigbelly']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        #Set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('andradej_chojoe', 'andradej_chojoe')
        
        repo.dropPermanent('andradej_chojoe.trashSch_transf')
        repo.createPermanent('andradej_chojoe.trashSch_transf')

        trashSchInfo = repo['andradej_chojoe.trashSch'].find()

        #SAMPLES THE DATA
        #trashSchInfo = trashSchInfo[:2000]

        processed_trashSchedules = project(trashSchInfo, processData)
        processed_trashSchedules = select(processed_trashSchedules, removeNoneValues)
        processed_trashSchedules = aggregate(processed_trashSchedules, set)
        processed_trashSchedules = dictionarify(processed_trashSchedules)
        
        for t in processed_trashSchedules:
            t = dict(t)
            repo['andradej_chojoe.trashSch_transf'].insert_one(t)
        
        endTime = datetime.datetime.now()
        return{'start':startTime, "end":endTime}
        
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('andradej_chojoe', 'andradej_chojoe')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/andradej_chojoe') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/andradej_chojoe') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        
        this_script = doc.agent('alg:andradej_chojoe#trashSchedules', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        trashSch_rsc = doc.entity('bdp:je5q-tbjf', {'prov:label':'Trash Schedules by Address', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_trashSch = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Trash Schedules by Address '})
        doc.wasAssociatedWith(get_trashSch, this_script)
        doc.usage(
            get_trashSch,
            trashSch_rsc,
            startTime,
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        
        trashSch = doc.entity('dat:andradej_chojoe#trashSch', {prov.model.PROV_LABEL:'Trash Schedules by Address', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trashSch, this_script)
        doc.wasGeneratedBy(trashSch, get_trashSch, endTime)
        doc.wasDerivedFrom(trashSch, trashSch_rsc, get_trashSch, get_trashSch, get_trashSch)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()
        
        return doc
    
trashSchedules.execute()
doc = trashSchedules.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


# In[ ]:



