
# coding: utf-8

# In[2]:

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import re

#set operations borrowed from lecture notes
def union(R, S):
    return R + S

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]

# Helper method for Code and Food Violations to count occurences
def aggregate(R, f):
    results = []
    
    #keys are all possible unique zip codes
    keys_zipcodes = {r[0] for r in R}
    keys_types = {r[1] for r in R}
    
    total_violations = []
    
    for key_zip in keys_zipcodes:
        for key_type in keys_types:
            total_violations = []
            for value in R:
                if key_zip == value[0]:
                    if key_type == value[1]:
                        total_violations.append(1)
            results.append((key_zip, key_type, f(total_violations)))
            
    return results 
    
# Helper method for Code Enforcement
def projectCodeViol(row):
    try:
        if row['zip'] and row['description']:
            zipcode = row['zip']
            description = row['description']
        else:
            return None
    except:
        return None

    #we append the newly contructed tuple format
    return (zipcode, description)

# Helper method for Code Enforcement
def compareCodeTypes(row):
    sanitation_types = ['extermination of insects, rodents and skunks',                        'illegal dumping',                        'improper storage trash',                        'storage of garbage & rubbish',                        'overfilling of barrel/dumpster',                        'trash illegally dump container']
    for san_type in sanitation_types:
        if row == None:
            return None
        if re.match(san_type,row[1].lower()):
            # need to categorize the same as Food Violations to merge later
            if (san_type == 'extermination of insects, rodents and skunks'):
                return (row[0], 'Insects  Rodents  Animals')
            return (row[0], san_type)
    return None

# Helper method for Code Enforcement to remove none and invalid
def removeNoneValues(row):
    if not row:
        return False
    elif row[0] == "00000":
        return False
    else:
        return True

# Helper method for Code Enforcement
def removeZeroOccurences(row):
    # if the count is 0 do not include in final set
    if row[2] <= 0:
        return False
    else:
        return True
        

# Helper method for Food Violations
def projectFoodViol(row):
    try: 
        if row['zip'] and row['violstatus'] and row['violdesc']:
            
            # takes into account error for zip codes with 4 length
            if len(row['zip']) < 5:
                zipcode = "0" + str(row['zip'])
            else:
                zipcode = row['zip']
                
            # only considers failed inspections
            if row['violstatus'] != "Fail":
                return None

            description = row['violdesc']
            
            return (zipcode, description)
        else:
            return None
    except:
        return None

# Helper method for Food Violations
def removeValues(row):
    if row == None:
        return False
    elif row[1] != 'Insects  Rodents  Animals':
        return False
    else:
        return True  
    
# serves as an aggregrate function for the unionized set of Code and Food
def merge(R, f):
    results = []
    
    #keys are all possible unique zip codes
    keys_zipcodes = {r[0] for r in R}
    keys_types = {r[1] for r in R}
    
    total_violations = []
    
    for key_zip in keys_zipcodes:
        for key_type in keys_types:
            total_violations = []
            for value in R:
                if key_zip == value[0]:
                    if key_type == value[1]:
                        total_violations.append(value[2])
            results.append((key_zip, key_type, f(total_violations)))
            
    return results 

#takes a list and translates its individual elements to dictionaries
def dictionarify(R):
    result = []
    for r in R:
        #result.update('zipcode': r[0], 'days': r[1]})
        result.append((('zipcode', r[0]), ('type', r[1]), ('count', r[2])))
        #print(result)
    return result

class codeViolations(dml.Algorithm):
    contributor = 'andradej_chojoe'
    reads = []
    writes = ['andrade_chojoe.codeViolations']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        #Set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('andradej_chojoe', 'andradej_chojoe')
        
        repo.dropPermanent('andradej_chojoe.codeViol_transf')
        repo.createPermanent('andradej_chojoe.codeViol_transf')

#---------- Code Enforcement processing------------------------------------------
        #Grabs health code enforcement dataset
        codeEnfInfo = repo['andradej_chojoe.codeEnf'].find()
        #samples data
        #codeEnfInfo = codeEnfInfo[:50]

        codeEnfInfo_filtered = project(codeEnfInfo, projectCodeViol)
        codeEnfInfo_filtered = project(codeEnfInfo_filtered, compareCodeTypes)
        codeEnfInfo_filtered = select(codeEnfInfo_filtered, removeNoneValues)
        codeEnfInfo_filtered = aggregate(codeEnfInfo_filtered, sum)
        codeEnfInfo_filtered = select(codeEnfInfo_filtered, removeZeroOccurences)
        
#--------------------------------------------------------------------------------

# ---------------Food Establishment processing------------------------------------

        foodInsInfo = repo['andradej_chojoe.foodEst'].find()
        #sample data
        #foodInsInfo = foodInsInfo[:50]

        # perform transformations 
        foodIns_filtered = project(foodInsInfo, projectFoodViol) #gets the appropriate columns
        foodIns_filtered = select(foodIns_filtered, removeValues) #removes rows with Null values
        foodIns_filtered = aggregate(foodIns_filtered, sum)
    #     print(foodIns_filtered)
# ---------------Food Establishment processing------------------------------------

        # merge datasets together
        sanitationViolations = union(codeEnfInfo_filtered, foodIns_filtered)
        sanitationViolations = merge(sanitationViolations, sum) #aggregate function
        sanitationViolations = select(sanitationViolations, removeZeroOccurences)
        
        sanitationViolations = dictionarify(sanitationViolations)
        #print(sanitationViolations) # done 
        
        for t in sanitationViolations:
            t = dict(t)
            repo['andradej_chojoe.codeViol_transf'].insert_one(t)
            
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
        
        this_script = doc.agent('alg:andradej_chojoe#codeViolations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        codeEnf_rsc = doc.entity('bdp:w39n-pvs8', {'prov:label':'Code Enforcement - Building and Property Violations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_codeEnf = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Code Enforcement - Building and Property Violations'})
        doc.wasAssociatedWith(get_codeEnf, this_script)
        doc.usage(
            get_codeEnf,
            codeEnf_rsc,
            startTime,
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        
        foodEst_rsc = doc.entity('bdp:427a-3cn5', {'prov:label':'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_foodEst = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Food Establishment Inspections'})
        doc.wasAssociatedWith(get_foodEst, this_script)
        doc.usage(
            get_foodEst,
            foodEst_rsc,
            startTime,
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        
        codeEnf = doc.entity('dat:andradej_chojoe#codeEnf', {prov.model.PROV_LABEL:'Code Enforcement - Building and Property Violations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(codeEnf, this_script)
        doc.wasGeneratedBy(codeEnf, get_codeEnf, endTime)
        doc.wasDerivedFrom(codeEnf, codeEnf_rsc, get_codeEnf, get_codeEnf, get_codeEnf)
        
        foodEst = doc.entity('dat:andradej_chojoe#foodEst', {prov.model.PROV_LABEL:'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(foodEst, this_script)
        doc.wasGeneratedBy(foodEst, get_foodEst, endTime)
        doc.wasDerivedFrom(foodEst, foodEst_rsc, get_foodEst, get_foodEst, get_foodEst)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()
        
        return doc

codeViolations.execute()
doc = codeViolations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


# In[ ]:



