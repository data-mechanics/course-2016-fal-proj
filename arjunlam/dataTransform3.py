import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crimePotholes(dml.Algorithm):
    contributor = 'arjunlam'
    reads = ['arjunlam.crime', 'arjunlam.potholes']
    writes = ['arjunlam.crimePotholes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arjunlam', 'arjunlam')

        repo.dropPermanent("crimePotholes")
        repo.createPermanent("crimePotholes")

        #Add up crimes/potholes based on zipcode
        crime = repo.arjunlam.crime
        potholes = repo.arjunlam.potholes
        collectionsArray = [crime, potholes]
        
        
        numCrimes = {} #store number of crimes per zipcode, the keys are the zipcodes
        numPotholes = {}

        result = []
        for collection in collectionsArray:
            for row in collection.find():
            
                if (collection == repo.arjunlam.crime):
                    zip = row['geo_location']['properties']['zipcode']
                    if zip not in numCrimes:
                        numCrimes[zip] = 1
                    else:
                        numCrimes[zip] += 1
                else:
                    zip = row['geo_location']['properties']['zipcode']
                    if zip not in numPotholes:
                        numPotholes[zip] = 1
                    else:
                        numPotholes[zip] += 1

        for zip in numCrimes:
            if zip in numPotholes:
                result.append({'zipcode': zip, 'Crime': numCrimes[zip], 'Potholes': numPotholes[zip]})
      
        repo['arjunlam.crimePotholes'].insert_many(result)
        

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

        this_script = doc.agent('alg:arjunlam#crimePotholes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        crime_entity = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime Incident Report', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        pothole_entity = doc.entity('bdp:wivc-syw7', {'prov:label':'Closed Pothole Cases', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_potholes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_potholes, this_script)
        
        doc.usage(get_crime, crime_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_potholes, pothole_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        crimePotholes = doc.entity('dat:arjunlam#crime', {prov.model.PROV_LABEL:'Crime And Potholes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimePotholes, this_script)
        doc.wasGeneratedBy(get_crime, get_crime, endTime)
        doc.wasGeneratedBy(get_potholes, get_potholes, endTime)
        doc.wasDerivedFrom(crimePotholes, crime_entity, get_crime, get_crime, get_crime)
        doc.wasDerivedFrom(crimePotholes, pothole_entity, get_potholes, get_potholes, get_potholes)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


crimePotholes.execute()
#doc = crimePotholes.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof