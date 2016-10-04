import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class liquorLicense(dml.Algorithm):
    contributor = 'cyung20_kwleung'
    reads = []
    writes = ['cyung20_kwleung.liquor']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        client = dml.pymongo.MongoClient()
        repo = client.repo 
        repo.authenticate("cyung20_kwleung", "cyung20_kwleung")
        
        url = 'https://data.cityofboston.gov/resource/g9d9-7sj6.json?$$app_token'    
        response = urllib.request.urlopen(url).read().decode("utf-8")           
        r = json.loads(response)
        repo.dropPermanent("liquor")
        repo.createPermanent("liquor")
        repo['cyung20_kwleung.liquor'].insert_many(r)

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

        this_script = doc.agent('alg:cyung20_kwleung#liquorLicense', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:g9d9-7sj6', {'prov:label':'Liquor Licenses', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_liquor = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_liquor, this_script)
            
        liquor = doc.entity('dat:cyung20_kwleung#liquor', {prov.model.PROV_LABEL:'Liquor Licenses', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(liquor, this_script)
        doc.wasGeneratedBy(liquor, get_liquor, endTime)
        doc.wasDerivedFrom(liquor, resource, liquor, get_liquor, get_liquor)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc
        
liquorLicense.execute()
doc = liquorLicense.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))