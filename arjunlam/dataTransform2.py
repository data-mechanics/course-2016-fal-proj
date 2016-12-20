import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class aggByZipcode(dml.Algorithm):
    contributor = 'arjunlam'
    reads = ['arjunlam.crime', 'arjunlam.closed311', 'arjunlam.potholes', 'arjunlam.hotline']
    writes = ['arjunlam.aggByZipData']

    #sum the crimes
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arjunlam', 'arjunlam')
        
        repo.dropPermanent("aggByZipData")
        repo.createPermanent("aggByZipData")

        #get 311 request by zipcode and add up the same 311 within a zipcode
        crime = repo.arjunlam.crime
        closed311 = repo.arjunlam.closed311
        potholes = repo.arjunlam.potholes
        hotline = repo.arjunlam.hotline
        collectionsArray = [crime, closed311, potholes, hotline]
        
        
        numCrimes = {"02108": 0, "02109": 0, "02110": 0, "02111": 0, "02113": 0, "02114": 0, "02115": 0, "02116": 0, "02118": 0, "02119": 0, "02120": 0, "02121": 0, "02122": 0, "02124": 0, "02125": 0, "02126": 0, "02127": 0, "02128": 0, "02129": 0, "02130": 0, "02131": 0, "02132": 0, "02134": 0, "02135": 0, "02136": 0, "02151": 0, "02152": 0, "02163": 0, "02199": 0, "02203": 0, "02210": 0, "02215": 0, "02467": 0, "02133": 0, "02222": 0} #store number of crimes per zipcode, the keys are the zipcodes
        numClosed311 = {"02108": 0, "02109": 0, "02110": 0, "02111": 0, "02113": 0, "02114": 0, "02115": 0, "02116": 0, "02118": 0, "02119": 0, "02120": 0, "02121": 0, "02122": 0, "02124": 0, "02125": 0, "02126": 0, "02127": 0, "02128": 0, "02129": 0, "02130": 0, "02131": 0, "02132": 0, "02134": 0, "02135": 0, "02136": 0, "02151": 0, "02152": 0, "02163": 0, "02199": 0, "02203": 0, "02210": 0, "02215": 0, "02467": 0, "02133": 0, "02222": 0}
        numPotholes = {"02108": 0, "02109": 0, "02110": 0, "02111": 0, "02113": 0, "02114": 0, "02115": 0, "02116": 0, "02118": 0, "02119": 0, "02120": 0, "02121": 0, "02122": 0, "02124": 0, "02125": 0, "02126": 0, "02127": 0, "02128": 0, "02129": 0, "02130": 0, "02131": 0, "02132": 0, "02134": 0, "02135": 0, "02136": 0, "02151": 0, "02152": 0, "02163": 0, "02199": 0, "02203": 0, "02210": 0, "02215": 0, "02467": 0, "02133": 0, "02222": 0}
        numHotline = {"02108": 0, "02109": 0, "02110": 0, "02111": 0, "02113": 0, "02114": 0, "02115": 0, "02116": 0, "02118": 0, "02119": 0, "02120": 0, "02121": 0, "02122": 0, "02124": 0, "02125": 0, "02126": 0, "02127": 0, "02128": 0, "02129": 0, "02130": 0, "02131": 0, "02132": 0, "02134": 0, "02135": 0, "02136": 0, "02151": 0, "02152": 0, "02163": 0, "02199": 0, "02203": 0, "02210": 0, "02215": 0, "02467": 0, "02133": 0, "02222": 0}


        for collection in collectionsArray:
            for row in collection.find():
            
                if (collection == repo.arjunlam.crime):
                    zip = row['geo_location']['properties']['zipcode']
                    numCrimes[zip] += 1
                elif (collection == repo.arjunlam.closed311):
                    zip = row['geo_location']['properties']['zipcode']
                    #print(zip)
                    numClosed311[zip] += 1
                elif (collection == repo.arjunlam.potholes):
                    zip = row['geo_location']['properties']['zipcode']
                    numPotholes[zip] += 1
                else:
                    zip = row['geo_location']['properties']['zipcode']
                    zip = '0' + zip
                    numHotline[zip] += 1
        
        #don't save zipcodes that have 0 for all of the violations/crime
        result = []
        for zip in numClosed311:
            if (numClosed311[zip] != 0) and (numCrimes[zip] != 0) and (numHotline[zip] != 0) and (numPotholes[zip] != 0):
                result.append({'zipcode': zip, 'Crime': numCrimes[zip], 'Closed311': numClosed311[zip], 'Potholes': numPotholes[zip], 'Hotline': numHotline[zip]})
            
            
        repo['arjunlam.aggByZipData'].insert_many(result)
        
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
        this_script = doc.agent('alg:arjunlam#aggByZipData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #Entity
        crime_entity = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime Incident Report', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        closed311_entity = doc.entity('bdp:wc8w-nujj', {'prov:label':'Closed 311 Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        hotline_entity = doc.entity('bdp:wc8w-nujj', {'prov:label':'Hotline Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        potholes_entity = doc.entity('bdp:wc8w-nujj', {'prov:label':'Potholes Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        #Activity
        get_aggByZipcode = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_aggByZipcode, this_script)
        
        doc.usage(get_aggByZipcode, crime_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_aggByZipcode, closed311_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_aggByZipcode, hotline_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_aggByZipcode, potholes_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        
        aggByZipData = doc.entity('dat:arjunlam#aggByZipData', {prov.model.PROV_LABEL:'Aggregate by zipcode', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(aggByZipData, this_script)
        doc.wasGeneratedBy(aggByZipData, get_aggByZipcode, endTime)
        doc.wasDerivedFrom(aggByZipData, crime_entity, get_aggByZipcode, get_aggByZipcode, get_aggByZipcode)
        doc.wasDerivedFrom(aggByZipData, closed311_entity, get_aggByZipcode, get_aggByZipcode, get_aggByZipcode)
        doc.wasDerivedFrom(aggByZipData, hotline_entity, get_aggByZipcode, get_aggByZipcode, get_aggByZipcode)
        doc.wasDerivedFrom(aggByZipData, potholes_entity, get_aggByZipcode, get_aggByZipcode, get_aggByZipcode)

        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


aggByZipcode.execute()
#doc = aggByZipData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof