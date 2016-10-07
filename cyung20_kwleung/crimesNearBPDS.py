import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
#from pprint import pprint

class crimesNearBPDS(dml.Algorithm):
    contributor = 'cyung20_kwleung'
    reads = ['cyung20_kwleung.crime', 'cyung20_kwleung.BPDS']
    writes = ['cyung20_kwleung.BPDS']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyung20_kwleung', 'cyung20_kwleung')
        
        data1 = repo['cyung20_kwleung.BPDS'].find()
        data2 = repo['cyung20_kwleung.crime'].find()

            
        # Retrieves all districts which had a crime incident report and stores them into a set
        districts_set = set()
        for document in data2:
            dictionary = dict(document)
            
            try:
                district = dictionary['district']
            except KeyError:
                district = None
                
            # Ignore crime incident reports that do not have a district listed
            if (district != None):
                districts_set.add(district)

                
        # Convert set into a list
        districts = list(districts_set)
        #print(districts)


        # Counts how many crime incidents were reported in each district, then adds the counts to their
        # corresponding districts in the pre-existing BPDS dataset
        index = 0
        for document in data1:
            district = districts[index]
            num_crimes_in_district = repo['cyung20_kwleung.crime'].count({'district': district})
            #print(district, num_crimes_in_district)
                
            repo['cyung20_kwleung.BPDS'].update_many({'_id': document['_id']},
                {'$set': {'num_crimes_in_district': num_crimes_in_district},})
            
            index += 1
            
         #prints out the dataset
#        for document in repo['cyung20_kwleung.BPDS'].find():
#            pprint(document)
            
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

          # Set up the database connection.
         client = dml.pymongo.MongoClient()
         repo = client.repo
         repo.authenticate('cyung20_kwleung', 'cyung20_kwleung')

         doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
         doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
         doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
         doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
         doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

         this_script = doc.agent('alg:cyung20_kwleung#crimesNearBPDS', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

         crime_resource = doc.entity('dat:cyung20_kwleung#crime', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
         BPDS_resource = doc.entity('dat:cyung20_kwleung#BPDS', {'prov:label':'Boston Police District Stations', prov.model.PROV_TYPE:'ont:DataSet'})

         this_merge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Crimes Near Boston Police District Stations', prov.model.PROV_TYPE:'ont:Computation'})

         doc.wasAssociatedWith(this_merge, this_script)
         doc.usage(this_merge, crime_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
         doc.usage(this_merge, BPDS_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

         crime_and_BPDS = doc.entity('dat:cyung20_kwleung#crimesNearBPDS', {prov.model.PROV_LABEL:'Crimes Near Boston Police District Stations', prov.model.PROV_TYPE:'ont:DataSet'})
         doc.wasAttributedTo(crime_and_BPDS, this_script)
         doc.wasGeneratedBy(crime_and_BPDS, this_merge, endTime)
         doc.wasDerivedFrom(crime_and_BPDS, crime_resource, this_merge, this_merge, this_merge)
         doc.wasDerivedFrom(crime_and_BPDS, BPDS_resource, this_merge, this_merge, this_merge)

         repo.record(doc.serialize()) # Record the provenance document.
         repo.logout()

         return doc


crimesNearBPDS.execute()
doc = crimesNearBPDS.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof