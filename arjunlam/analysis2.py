import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import scipy.stats

class crimeAnalysis2(dml.Algorithm):
    contributor = 'arjunlam'
    reads = ['arjunlam.crime311']
    writes = ['arjunlam.crimeVS311']
    
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
        repo.dropPermanent("crimeVS311")
        repo.createPermanent("crimeVS311")

        #crime/potholes dataset
        crime311 = repo.arjunlam.crime311
        
        #crime/311 data is grouped according to zipcode
        crimeVs311 = [] #store (x, y) pairs where x = # crimes and y = # closed 311 requests
        
        result = []
        
        x = 0 #gets all the results from find()
        if trial == True:
            x = 3 #limit to 3 results if trial is True
            
        for row in repo.arjunlam.crime311.find().limit(x):
            #for a given zipcode get # of crimes and # of potholes
            x = row['Crime']
            y = row['Closed311']
            crimeVs311.append((x, y))
        
        #calculate statistics and store in result array
        x = [xi for (xi, yi) in crimeVs311]
        y = [yi for (xi, yi) in crimeVs311]
        p311 = scipy.stats.pearsonr(x, y)
        avgCrime = crimeAnalysis2.avg(x)
        avg311 = crimeAnalysis2.avg(y)
        result.append({'Crime vs Closed311 corr': p311[0], 'Crime Vs Closed311 p-val': p311[1],'Average number of Crimes': avgCrime, 'Average number of 311 Request': avg311})
        
        repo['arjunlam.crimeVS311'].insert_many(result)

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
        this_script = doc.agent('alg:arjunlam#crimeAnalysis2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #Entity
        crime311_entity = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime and closed 311 per zipcode', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #Activity
        get_crimeAnalysis2 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_crimeAnalysis2, this_script)
        doc.usage(get_crimeAnalysis2, crime311_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        crimeVS311 = doc.entity('dat:arjunlam#crimeVS311', {prov.model.PROV_LABEL:'Crime And Closed 311 Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeVS311, this_script)
        doc.wasGeneratedBy(crimeVS311, get_crimeAnalysis2, endTime)
        doc.wasDerivedFrom(crimeVS311, crime311_entity, get_crimeAnalysis2, get_crimeAnalysis2, get_crimeAnalysis2)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


crimeAnalysis2.execute()
#doc = crimeAnalysis2.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof