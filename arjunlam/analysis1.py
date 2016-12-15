import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import scipy.stats

class crimeAnalysis1(dml.Algorithm):
    contributor = 'arjunlam'
    reads = ['arjunlam.crimePotholes']
    writes = ['arjunlam.crimeVSPotholes']
    
    def avg(x): # Average
        return sum(x)/len(x)
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arjunlam', 'arjunlam')

        #create new collection
        repo.dropPermanent("crimeVSPotholes")
        repo.createPermanent("crimeVSPotholes")

        #crime/potholes dataset
        crimePotholes = repo.arjunlam.crimePotholes
        
        #crime/potholes data is grouped according to zipcode
        crimeVsPotholes = [] #store (x, y) pairs where x = # crimes and y = # potholes
        
        result = []
        
        
        x = 0 #gets all the results from find()
        if trial == True:
            x = 3 #limit to 3 results if trial is True
            
        for row in repo.arjunlam.crimePotholes.find().limit(x):
            #for a given zipcode get # of crimes and # of potholes
            x = row['Crime']
            y = row['Potholes']
            crimeVsPotholes.append((x, y))
        
        #calculate statistics and store in result array
        x = [xi for (xi, yi) in crimeVsPotholes]
        y = [yi for (xi, yi) in crimeVsPotholes]
        pPot = scipy.stats.pearsonr(x, y)
        avgCrime = crimeAnalysis1.avg(x)
        avgPotholes = crimeAnalysis1.avg(y)
        result.append({'Crime vs Potholes corr': pPot[0], 'Crime Vs Potholes p-val': pPot[1],'Average number of Crimes': avgCrime, 'Average number of Potholes': avgPotholes})
        
        
        repo['arjunlam.crimeVSPotholes'].insert_many(result)

        repo.logout()

        endTime = datetime.datetime.now()

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
        repo.authenticate('arjunlam', 'arjunlam')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        #Agent
        this_script = doc.agent('alg:arjunlam#crimeAnalysis1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #Entity
        crimePotholes_entity = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime and Potholes per zipcode', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #Activity
        get_crimeAnalysis1 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_crimeAnalysis1, this_script)
        doc.usage(get_crimeAnalysis1, crimePotholes_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        crimeVSPotholes = doc.entity('dat:arjunlam#crimeVSPotholes', {prov.model.PROV_LABEL:'Crime And Potholes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeVSPotholes, this_script)
        doc.wasGeneratedBy(crimeVSPotholes, get_crimeAnalysis1, endTime)
        doc.wasDerivedFrom(crimeVSPotholes, crimePotholes_entity, get_crimeAnalysis1, get_crimeAnalysis1, get_crimeAnalysis1)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


crimeAnalysis1.execute()
#doc = crimeAnalysis1.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof