import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geocoder
from collections import Counter

class hubwayBigBelly(dml.Algorithm):
    contributor = 'emilyh23_ktan_ngurung_yazhang'
    reads = ['emilyh23_ktan_ngurung_yazhang.colleges', 'emilyh23_ktan_ngurung_yazhang.busStops']
    writes = ['emilyh23_ktan_ngurung_yazhang.hubwayBigBellyCounts']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emilyh23_ktan_ngurung_yazhang', 'emilyh23_ktan_ngurung_yazhang')

        # Get bus stop and college location data
        hubways = repo.emilyh23_ktan_ngurung_yazhang.hubways.find() 
        bigBelly = repo.emilyh23_ktan_ngurung_yazhang.bigBelly.find()
        hbbDict = {}

        # Commented block below can only be a when query limit for geocoder library has not been used up
        '''
        for doc in bigBelly: 
            docDict = dict(doc)
            for i in range(len(docDict['data'])): 
                lastIndex = docDict['data'][i][-1]
                coordinates = [lastIndex[1], lastIndex[2]]

                address = geocoder.google(coordinates, method='reverse')
                zipcode = str(address.postal)

                if zipcode != 'None' and zipcode not in hbbDict:
                    hbbDict[zipcode] = {'bigBellyCount' : 1, 'hubwayCount' : 0}

                elif zipcode != 'None' and zipcode in hbbDict:
                    hbbDict[zipcode]['bigBellyCount'] += 1

                else:
                     pass

        for station in hubways:
            coordinates = station['fields']['coordinates']
            address = geocoder.google(coordinates, method='reverse')
            zipcode = str(address.postal)
            print(address)

            if zipcode != 'None' and zipcode in hbbDict:
                hbbDict[zipcode]['hubwayCount'] += 1

            elif zipcode != 'None' and zipcode not in hbbDict:
                hbbDict[zipcode] = {'bigBellyCount' : 0, 'hubwayCount': 1}

            else:
                pass
        '''

        b = {'02124': {'bigBellyCount': 1, 'hubwayCount': 0}, '02111': {'bigBellyCount': 27, 'hubwayCount': 0}, '02114': {'bigBellyCount': 27, 'hubwayCount': 0}, '02446': {'bigBellyCount': 4, 'hubwayCount': 0}, '02215': {'bigBellyCount': 25, 'hubwayCount': 0}, '02110': {'bigBellyCount': 23, 'hubwayCount': 0}, '02119': {'bigBellyCount': 1, 'hubwayCount': 0}, '02135': {'bigBellyCount': 14, 'hubwayCount': 0}, '02445': {'bigBellyCount': 1, 'hubwayCount': 0}, '02134': {'bigBellyCount': 15, 'hubwayCount': 0}, '02120': {'bigBellyCount': 8, 'hubwayCount': 0}, '02113': {'bigBellyCount': 14, 'hubwayCount': 0}, '02122': {'bigBellyCount': 2, 'hubwayCount': 0}, '02467': {'bigBellyCount': 1, 'hubwayCount': 0}, '02109': {'bigBellyCount': 14, 'hubwayCount': 0}, '02130': {'bigBellyCount': 3, 'hubwayCount': 0}, '02115': {'bigBellyCount': 24, 'hubwayCount': 0}, '02118': {'bigBellyCount': 26, 'hubwayCount': 0}, '02128': {'bigBellyCount': 7, 'hubwayCount': 0}, '02108': {'bigBellyCount': 22, 'hubwayCount': 0}, '02199': {'bigBellyCount': 3, 'hubwayCount': 0}, '02116': {'bigBellyCount': 38, 'hubwayCount': 0}, '02125': {'bigBellyCount': 5, 'hubwayCount': 0}, '02210': {'bigBellyCount': 12, 'hubwayCount': 0}, '02127': {'bigBellyCount': 1, 'hubwayCount': 0}}
        h = {'02110': {'bigBellyCount': 0, 'hubwayCount': 2}, '02144': {'bigBellyCount': 0, 'hubwayCount': 1}, '02119': {'bigBellyCount': 0, 'hubwayCount': 1}, '02125': {'bigBellyCount': 0, 'hubwayCount': 1}, '02210': {'bigBellyCount': 0, 'hubwayCount': 1}, '02138': {'bigBellyCount': 0, 'hubwayCount': 2}, '02140': {'bigBellyCount': 0, 'hubwayCount': 1}, '02114': {'bigBellyCount': 0, 'hubwayCount': 1}, '02115': {'bigBellyCount': 0, 'hubwayCount': 2}, '02116': {'bigBellyCount': 0, 'hubwayCount': 1}, '02139': {'bigBellyCount': 0, 'hubwayCount': 1}}

        # Merging dictionaries above
        for zipcode, value in b.items():
            b[zipcode] = Counter(value)

        for zipcode, value in h.items():
            h[zipcode] = Counter(value)

        for zipcode, value in b.items():
            if zipcode in h.keys():
                hbbDict[zipcode] = b[zipcode] + h[zipcode]
            else:
                hbbDict[zipcode] = b[zipcode]

        for zipcode, value in h.items():
            if zipcode not in hbbDict:
                hbbDict[zipcode] = h[zipcode]

        for zipcode, value in hbbDict.items():
            hbbDict[zipcode] = dict(value)
  
        # Convert dictionary into JSON object 
        data = json.dumps(hbbDict, sort_keys=True, indent=2)
        r = json.loads(data)

        # Create new dataset called hubwayBigBellyCounts
        repo.dropPermanent("hubwayBigBellyCounts")
        repo.createPermanent("hubwayBigBellyCounts")
        repo['emilyh23_ktan_ngurung_yazhang.hubwayBigBellyCounts'].insert_one(r)

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
        repo.authenticate('emilyh23_ktan_ngurung_yazhang', 'emilyh23_ktan_ngurung_yazhang')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:emilyh23_ktan_ngurung_yazhang#hubwayBigBelly', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        hubway_resource = doc.entity('dat:emilyh23_ktan_ngurung_yazhang/hubway-stations-in-boston', {'prov:label':'Hubway Stations in Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        bigBelly_resource = doc.entity('bdp:42qi-w8d7', {'prov:label':'Big Belly Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, hubway_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )
        doc.usage(this_run, bigBelly_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        hubwayBigBelly = doc.entity('dat:emilyh23_ktan_ngurung_yazhang#hubwayBigBelly', {prov.model.PROV_LABEL:'Number of Hubways and Big Belly for Each Zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubwayBigBelly, this_script)
        doc.wasGeneratedBy(hubwayBigBelly, this_run, endTime)
        doc.wasDerivedFrom(hubwayBigBelly, hubway_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(hubwayBigBelly, bigBelly_resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

hubwayBigBelly.execute() 
doc = hubwayBigBelly.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof 
