import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import scipy.stats
import numpy as np
import matplotlib.pyplot as plt
from scipy.cluster.vq import kmeans2, whiten
from matplotlib.gridspec import GridSpec

class crimeAnalysis2(dml.Algorithm):
    contributor = 'arjunlam'
    reads = ['arjunlam.crime', 'arjunlam.closed311']
    writes = []
    
    
    def getCoords(collection, x):
        arr = []
        for row in collection.find().limit(x):
            x = row['geo_location']['geometry']['coordinates'][1]
            y = row['geo_location']['geometry']['coordinates'][0]
            arr.append((float(x), float(y)))
        return arr
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arjunlam', 'arjunlam')


        #dataset
        crime = repo.arjunlam.crime
        closed311 = repo.arjunlam.closed311
        collectionsArray = [crime, closed311]
       
       
        #longitude = x
        #latitude = y
        
        x = 0 #gets all the results from find()
        if trial == True:
            x = 50000
         
        #get coordinates
        crimeCord  = crimeAnalysis2.getCoords(collectionsArray[0], x)
        closed311Cord = crimeAnalysis2.getCoords(collectionsArray[1], x)
        
        crimeCord = sorted(crimeCord)[15:] #did this because some of the coordinates are (-1, -1)
        
        #do k means clustering
        k = 5
        it = 3000
        kCentroidsCrime, yCrime = kmeans2(crimeCord, k, iter = it, minit='points') 
        kCentroids311, y311 = kmeans2(closed311Cord, k, iter = it, minit='points')         

        
        #plot the centroids  
        fig = plt.figure()
        plt.scatter(*zip(*crimeCord)) #plot locations of crimes
        ll = plt.scatter(*zip(*kCentroidsCrime), c='r', s=50)
        lh = plt.scatter(*zip(*kCentroids311), c='g', s=50)
        plt.legend((ll, lh), ("Crime centroids", "311 centroids"), ncol=1, loc="upper left", scatterpoints=1)
        plt.title("Crime vs 311 Centroid Locations")
        #plt.savefig('crimevs311centroidslocationk=5_2.png')
        plt.show() 

        '''
        #PLOTTING HISTOGRAMS
        data = crimeCord
        fig = plt.figure()
        plt.title("Crime Locations")
        gs = GridSpec(8,8)
        plt.subplots_adjust(wspace=0, hspace=0)
        ax_joint = fig.add_subplot(gs[2:8,0:6])
        ax_marg_x = fig.add_subplot(gs[0:2,0:6])
        ax_marg_y = fig.add_subplot(gs[2:8,6:8])
        x, y = zip(*data)
        ax_joint.scatter(x,y)
        numBins = 60
        ax_marg_x.hist(x,numBins)
        ax_marg_y.hist(y,numBins,orientation="horizontal")
        ax_marg_y.set_xlabel('Hist')
        ax_marg_x.set_ylabel('Hist')
        ax_joint.set_xlabel('Longitude')
        ax_joint.set_ylabel('Latitude')
        
        #get rid of ticks
        ax_marg_y.set_xticklabels([])
        ax_marg_y.set_yticklabels([])
        ax_marg_x.set_xticklabels([])
        ax_marg_x.set_yticklabels([])
        ax_joint.set_xticklabels([])
        ax_joint.set_yticklabels([])

        plt.savefig('CrimeLocationsHistogram.png')
        plt.show()
        '''

        
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
        crime_entity = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        closed311_entity = doc.entity('bdp:29yf-ye7n', {'prov:label':'Closed 311 coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #Activity
        get_crimeAnalysis2 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_crimeAnalysis2, this_script)
        doc.usage(get_crimeAnalysis2, crime_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_crimeAnalysis2, closed311_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


crimeAnalysis2.execute()
#doc = crimeAnalysis2.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof