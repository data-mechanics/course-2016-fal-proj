import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'ktan_ngurung'
    reads = []
    writes = ['ktan_ngurung.bigBelly', 'ktan_ngurung.colleges', 'ktan_ngurung.hubways', 'ktan_ngurung.busStops', 'ktan_ngurung.tStops']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')

        repo.dropPermanent("tRidershipLocation")
        repo.createPermanent("tRidershipLocation")

        # Get ridership and tstop location data
        ridership = repo.ktan_ngurung.ridership.find()
        tstops = repo.ktan_ngurung.tStops.find()
        entries = {}

        # Build dictionary of number of entries for each train stop
        for doc in ridership:
            docDict1 = dict(doc)
            for station in docDict1['stations']:
                if station['title'] not in entries:
                    entries[station['title']] = station['entries']

        # Update each train stop location with associated number of entries
        for doc in tstops:
            docDict2 = dict(doc)
            try: 
                for station in docDict2['stations']:
                    if station['title'] in entries:
                        station['entries'] = entries[station['title']]
                        res = repo.ktan_ngurung.ridershipLocation.insert_one(station)
            # KeyError occurs when end of dataset is reached, so pass
            except KeyError:
                pass
    
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
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('ede', 'http://erikdemaine.org/maps/')
        doc.add_namespace('mbt', 'http://www.mbta.com/about_the_mbta/document_library/')

        this_script = doc.agent('alg:ktan_ngurung#landmarkLocations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        tStops_resource = doc.entity('ede:mbta', {'prov:label':'T-Stop Locations', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'yaml'})
        ridership_resource = doc.entity('mbt:?search=blue+book&submit_document_search=Search+Library', {'prov:label':'Boston 2014 Bluebook', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'pdf'})
        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, tStops_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )
        doc.usage(this_run, ridership_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        ridershipLocation = doc.entity('dat:ktan_ngurung#ridershipLocation', {prov.model.PROV_LABEL:'Number of Entries for Each Train Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(ridershipLocation, this_script)
        doc.wasGeneratedBy(ridershipLocation, this_run, endTime)
        doc.wasDerivedFrom(ridershipLocation, tStops_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(ridershipLocation, ridership_resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc 

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
