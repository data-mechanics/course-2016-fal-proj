import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class streetlightLocations(dml.Algorithm):
    contributor = 'cyung20_kwleung'
    reads = []
    writes = ['cyung20_kwleung.lights']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Streetlight Locations'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyung20_kwleung', 'cyung20_kwleung')

        url = 'https://data.cityofboston.gov/resource/fbdp-b7et.json?$$app_token'

        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("lights")
        repo.createPermanent("lights")
        repo['cyung20_kwleung.lights'].insert_many(r)

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

         this_script = doc.agent('alg:cyung20_kwleung#streetlightLocations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
         resource = doc.entity('bdp:fbdp-b7et', {'prov:label':'Streetlight Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
         get_lights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
         doc.wasAssociatedWith(get_lights, this_script)
         doc.usage(get_lights, resource, startTime, None,
                 {prov.model.PROV_TYPE:'ont:Retrieval'}
             )

         lights = doc.entity('dat:cyung20_kwleung#streetlightLocations', {prov.model.PROV_LABEL:'Streetlight Locations', prov.model.PROV_TYPE:'ont:DataSet'})
         doc.wasAttributedTo(lights, this_script)
         doc.wasGeneratedBy(lights, get_lights, endTime)
         doc.wasDerivedFrom(lights, resource, get_lights, get_lights, get_lights)

         repo.record(doc.serialize()) # Record the provenance document.
         repo.logout()

         return doc

streetlightLocations.execute()
doc = streetlightLocations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof