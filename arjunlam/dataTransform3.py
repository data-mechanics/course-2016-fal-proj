import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class aggByNeigh(dml.Algorithm):
    contributor = 'arjunlam'
    reads = ['arjunlam.closed311', 'arjunlam.potholes', 'arjunlam.develop']
    writes = ['arjunlam.aggByNeighData']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arjunlam', 'arjunlam')
        
        repo.dropPermanent("aggByNeighData")
        repo.createPermanent("aggByNeighData")

        #Add up crimes/potholes based on zipcode
        closed311 = repo.arjunlam.closed311
        potholes = repo.arjunlam.potholes
        develop = repo.arjunlam.develop
        collectionsArray = [closed311, potholes, develop]
        
        
        numClosed311 = {} #store number of crimes per zipcode, the keys are the zipcodes
        numPotholes = {}
        numDevelop = {}

        result = []
        for collection in collectionsArray:
            for row in collection.find():
            
                if (collection == repo.arjunlam.closed311) and ('neighborhood' in row):
                    n = row['neighborhood']
                    if n not in numClosed311:
                        numClosed311[n] = 1
                    else:
                        numClosed311[n] += 1
                elif (collection == repo.arjunlam.develop) and ('neigh' in row):
                    n = row['neigh']
                    if n not in numDevelop:
                        numDevelop[n] = 1
                    else:
                        numDevelop[n] += 1
                else:
                    if ('neighborhood' in row):
                        n = row['neighborhood']
                        if n not in numPotholes:
                            numPotholes[n] = 1
                        else:
                            numPotholes[n] += 1

        for n in numClosed311:
            if n in numPotholes:
                if n not in numDevelop:
                    result.append({'Neighborhood': n, 'Closed311': numClosed311[n], 'Potholes': numPotholes[n], 'Develop': 0})
                else:
                    result.append({'Neighborhood': n, 'Closed311': numClosed311[n], 'Potholes': numPotholes[n], 'Develop': numDevelop[n]})
      
        repo['arjunlam.aggByNeighData'].insert_many(result)
        

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
        this_script = doc.agent('alg:arjunlam#aggByNeighData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #Entity
        closed311_entity = doc.entity('bdp:29yf-ye7n', {'prov:label':'Closed 311 Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        potholes_entity = doc.entity('bdp:wivc-syw7', {'prov:label':'Closed Pothole Cases', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        develop_entity = doc.entity('bdp:wivc-syw7', {'prov:label':'Boston development properties', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #Activity
        get_aggByNeighData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_aggByNeighData, this_script)
        
        doc.usage(get_aggByNeighData, closed311_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_aggByNeighData, potholes_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_aggByNeighData, develop_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        aggByNeighData = doc.entity('dat:arjunlam#aggByNeighData', {prov.model.PROV_LABEL:'Aggregrate according to neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggByNeighData, this_script)
        doc.wasGeneratedBy(aggByNeighData, get_aggByNeighData, endTime)
        doc.wasDerivedFrom(aggByNeighData, closed311_entity, get_aggByNeighData, get_aggByNeighData, get_aggByNeighData)
        doc.wasDerivedFrom(aggByNeighData, potholes_entity, get_aggByNeighData, get_aggByNeighData, get_aggByNeighData)
        doc.wasDerivedFrom(aggByNeighData, develop_entity, get_aggByNeighData, get_aggByNeighData, get_aggByNeighData)


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


aggByNeigh.execute()
#doc = aggByNeighData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof