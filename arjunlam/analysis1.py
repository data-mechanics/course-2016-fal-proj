import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import scipy.stats
import matplotlib.pyplot as plt
import numpy as np

class crimeAnalysis1(dml.Algorithm):
    contributor = 'arjunlam'
    reads = ['arjunlam.aggByZipData']
    writes = ['arjunlam.crimeCorr']
    
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
        repo.dropPermanent("crimeCorr")
        repo.createPermanent("crimeCorr")
        

        #zip code dataset
        zipData = repo.arjunlam.aggByZipData
        
        
        #data is grouped according to zipcode
        crimeVsPotholes = [] #store (x, y) pairs where x = # crimes and y = # potholes
        crimeVsHotline = []
        crimeVsClosed311 = []
        crimeVsAll = [] #add up potholes, hotline and 311 for a given zipcode
        
        
        x = 0 #gets all the results from find()
        if trial == True:
            x = 3 #limit to 3 results if trial is True
            
            
        for row in zipData.find().limit(x):
            #for a given zipcode get # of crimes
            x = row['Crime']
            if x != 0: #include only zipcodes that have crimes (because crimes data has fewer zipcodes than the service requests)
                y = row['Potholes']
                z = row['Hotline']
                u = row['Closed311']
                v = y + z + u
                crimeVsPotholes.append((x, y))
                crimeVsHotline.append((x, z))
                crimeVsClosed311.append((x, u))
                crimeVsAll.append((x, v))
        
        result = []
        plotX = []
        plotY = []
        pPotResult = []
        #calculate correlation
        temp = [crimeVsPotholes, crimeVsHotline, crimeVsClosed311, crimeVsAll]
        names = ['Crime Vs Potholes', 'Crime Vs Hotline', 'Crime Vs Closed311', 'Crime Vs All']
        names2 = ['Average num potholes', 'Average num hotline', 'Average num closed311', 'Total']
        for i in range(0, len(temp)):
            x = [xi for (xi, yi) in temp[i]]
            y = [yi for (xi, yi) in temp[i]]
            plotX.append(x)
            plotY.append(y)
            pPot = scipy.stats.pearsonr(x, y)
            pPotResult.append(pPot[0])
            
            #averages
            averageC = crimeAnalysis1.avg(x)
            average = crimeAnalysis1.avg(y)
            result.append({names[i] + " corr": pPot[0], names[i] + ' p-val': pPot[1], 'Average num crimes': averageC, names2[i]: average})

        '''
        #print the correlation graphs using matplot
        q = 2
        plt.scatter(plotX[q], plotY[q])
        z = np.polyfit(plotX[q], plotY[q], 1)
        p = np.poly1d(z)
        plt.plot(plotX[q],p(x),"r--")
        plt.title("Crime vs 311 | Correlation: -0.007")
        plt.ylabel('Number of 311', fontsize=16)
        plt.xlabel('Number of Crimes', fontsize=16)
        plt.savefig('crimeVs311Corr.png')
        plt.show()
        '''
        
        repo['arjunlam.crimeCorr'].insert_many(result)

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
        zipData_entity = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime and requests by zipcode', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #Activity
        get_crimeAnalysis1 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_crimeAnalysis1, this_script)
        doc.usage(get_crimeAnalysis1, zipData_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        crimeCorr = doc.entity('dat:arjunlam#crimeCorr', {prov.model.PROV_LABEL:'Correlation coefficients', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeCorr, this_script)
        doc.wasGeneratedBy(crimeCorr, get_crimeAnalysis1, endTime)
        doc.wasDerivedFrom(crimeCorr, zipData_entity, get_crimeAnalysis1, get_crimeAnalysis1, get_crimeAnalysis1)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


crimeAnalysis1.execute()
#doc = crimeAnalysis1.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof