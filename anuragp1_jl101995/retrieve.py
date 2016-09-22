import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrieve(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = []
    writes = ['anuragp1_jl101995.waze'] # add remaining datasets in here

    @staticmethod
    def execute(Trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')
        
        # Boston Waze Jam Data
        url = "https://data.cityofboston.gov/resource/dih6-az4h.json"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        # print(json.dumps(r, sort_keys=True, indent=2))
        # repo.dropPermanent("waze")
        repo.createPermanent("waze")
        repo['anuragp1_jl101995.waze'].insert_many(r)
        
        # Cambridge Crashes
        url = "https://data.cambridgema.gov/resource/39tu-m8zx.json"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        # print(json.dumps(r, sort_keys=True, indent=2))
        # repo.dropPermanent("waze")
        repo.createPermanent("crashes")
        repo['anuragp1_jl101995.crashes'].insert_many(r)
        
    
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
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/')

        this_script = doc.agent('alg:anuragp1_jl101995#retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        # Boston Waze Jam Data 
        resource = doc.entity('bdp:dih6-az4h', {'prov:label':'Waze Jam Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_waze = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_waze, this_script)
        doc.usage(get_waze, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'} )
        # Cambridge Crashes
        cambridge_resource = doc.entity('cdp:39tu-m8zx', {'prov:label':'Cambridge Crashes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crashes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crashes, this_script)
        doc.usage(get_crashes, cambridge_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'} )
                      
        waze = doc.entity('dat:anuragp1_jl101995#waze', {prov.model.PROV_LABEL:'Waze Jam Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(waze, this_script)
        doc.wasGeneratedBy(waze, get_waze, endTime)
        doc.wasDerivedFrom(waze, resource, get_waze, get_waze, get_waze)

        crashes = doc.entity('dat:anuragp1_jl101995#crashes', {prov.model.PROV_LABEL:'Cambridge Crashes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crashes, this_script)
        doc.wasGeneratedBy(crashes, get_crashes, endTime)
        doc.wasDerivedFrom(crashes, cambridge_resource, get_crashes, get_crashes, get_crashes)
        
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


retrieve.execute()
doc = retrieve.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


