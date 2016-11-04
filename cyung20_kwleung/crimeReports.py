import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crimeReports(dml.Algorithm):
    contributor = 'cyung20_kwleung'
    reads = []
    writes = ['cyung20_kwleung.crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Streetlight Locations'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyung20_kwleung', 'cyung20_kwleung')

        url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json?$$app_token'

        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("crime")
        repo.createPermanent("crime")
        repo['cyung20_kwleung.crime'].insert_many(r)

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

         this_script = doc.agent('alg:cyung20_kwleung#crimeReports', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
         resource = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
         get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
         doc.wasAssociatedWith(get_crime, this_script)
         doc.usage(get_crime, resource, startTime, None,
                 {prov.model.PROV_TYPE:'ont:Retrieval'}
             )

         crime = doc.entity('dat:cyung20_kwleung#crimeReports', {prov.model.PROV_LABEL:'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
         doc.wasAttributedTo(crime, this_script)
         doc.wasGeneratedBy(crime, get_crime, endTime)
         doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

         repo.record(doc.serialize()) # Record the provenance document.
         repo.logout()

         return doc

crimeReports.execute()
doc = crimeReports.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof