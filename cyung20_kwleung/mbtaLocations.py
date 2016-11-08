import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class mbtaLocations(dml.Algorithm):
    contributor = 'cyung20_kwleung'
    reads = []
    writes = ['cyung20_kwleung.stop']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Locations of all MBTA T Stops'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyung20_kwleung', 'cyung20_kwleung')

        url = 'http://datamechanics.io/data/cyung20_kwleung/mbta-t-stops.json'

        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("stop")
        repo.createPermanent("stop")
        repo['cyung20_kwleung.stop'].insert_many(r)

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
         repo.authenticate('cyung20_kwleung', 'cyung20_kwleung')

         doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
         doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
         doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
         doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
         doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

         this_script = doc.agent('alg:cyung20_kwleung#mbtaLocations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
         resource = doc.entity('dat:cyung20_kwleung/mbta-t-stops', {'prov:label':'MBTA T Stop Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
         get_stop = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
         doc.wasAssociatedWith(get_stop, this_script)
         doc.usage(get_stop, resource, startTime, None,
                 {prov.model.PROV_TYPE:'ont:Retrieval'}
             )

         stop = doc.entity('dat:cyung20_kwleung#mbtaLocations', {prov.model.PROV_LABEL:'MBTA T Stop Locations', prov.model.PROV_TYPE:'ont:DataSet'})
         doc.wasAttributedTo(stop, this_script)
         doc.wasGeneratedBy(stop, get_stop, endTime)
         doc.wasDerivedFrom(stop, resource, get_stop, get_stop, get_stop)

         repo.record(doc.serialize()) # Record the provenance document.
         repo.logout()

         return doc

mbtaLocations.execute()
doc = mbtaLocations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof