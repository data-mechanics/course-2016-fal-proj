import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crime311(dml.Algorithm):
    contributor = 'arjunlam'
    reads = ['arjunlam.crime', 'arjunlam.closed311']
    writes = ['arjunlam.crime311']

    #sum the crimes
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arjunlam', 'arjunlam')
        
        repo.dropPermanent("crime311")
        repo.createPermanent("crime311")

        #get 311 request by zipcode and add up the same 311 within a zipcode
        crime = repo.arjunlam.crime
        closed311 = repo.arjunlam.closed311
        collectionsArray = [crime, closed311]
        
        
        numCrimes = {} #store number of crimes per zipcode, the keys are the zipcodes
        numClosed311 = {}

        crimeClosed311 = [numCrimes, numClosed311]
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
                    if zip not in numClosed311:
                        numClosed311[zip] = 1
                    else:
                        numClosed311[zip] += 1

        numCrimes['data_type'] = 'Crime'
        numClosed311['data_type'] = 'Closed 311'
      
        repo['arjunlam.crime311'].insert_many(crimeClosed311)
        
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

        this_script = doc.agent('alg:arjunlam#crime311', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        crime_entity = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime Incident Report', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        closed311_entity = doc.entity('bdp:wc8w-nujj', {'prov:label':'Closed 311 Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_closed311 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_closed311, this_script)
        
        doc.usage(get_crime, crime_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_closed311, closed311_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
        
        crimeClosed311 = doc.entity('dat:arjunlam#crime', {prov.model.PROV_LABEL:'Crime Closed 311', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeClosed311, this_script)
        doc.wasGeneratedBy(get_crime, get_crime, endTime)
        doc.wasGeneratedBy(get_closed311, get_closed311, endTime)
        doc.wasDerivedFrom(get_crime, crime_entity, get_crime, get_crime, get_crime)
        doc.wasDerivedFrom(get_closed311, closed311_entity, get_closed311, get_closed311, get_closed311)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


crime311.execute()
#doc = crime311.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof