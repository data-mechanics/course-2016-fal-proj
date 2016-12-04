import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = []
    writes = ['ckarjadi_johnnyg7.lost', 'ckarjadi_johnnyg7.found']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')

        url = 'https://data.cityofboston.gov/resource/n7za-nsjh.json?$limit=20&$where=gross_tax%20%3E%200'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print(s)
        repo.dropPermanent("propVal")
        repo.createPermanent("propVal")
        repo['ckarjadi_johnnyg7.propVal'].insert_many(r)
        
        url = 'https://data.cityofboston.gov/resource/4tie-bhxw.json?$limit=20'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print(s)
        repo.dropPermanent("foodPan")
        repo.createPermanent("foodPan")
        repo['ckarjadi_johnnyg7.foodPan'].insert_many(r)

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
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        #<ckarjadi_johnnyg7>#<somefile_name>
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ckarjadi_johnnyg7#combine', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:g5b5-xrwi', {'prov:label':'Property Values 2016', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_propVal = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_foodPan = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_propVal, this_script)
        doc.wasAssociatedWith(get_foodPan, this_script)        
        doc.usage(get_propVal, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?$where=gross_tax%20%3E%200'
                }
            )
        doc.usage(get_propVal, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'???'
                }
            )
        prop_Val = doc.entity('dat:ckarjadi_johnnyg7#prop_Val', {prov.model.PROV_LABEL:'Property Value 2016', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prop_Val, this_script)
        doc.wasGeneratedBy(prop_Val, get_propVal, endTime)
        doc.wasDerivedFrom(prop_Val, resource, get_propVal, get_propVal, get_propVal)
        
        foodPan = doc.entity('dat:ckarjadi_johnnyg7#foodPan', {prov.model.PROV_LABEL:'Food Pantries', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(foodPan, this_script)
        doc.wasGeneratedBy(foodPan, get_foodPan, endTime)
        doc.wasDerivedFrom(foodPan, resource, get_foodPan, get_foodPan, get_foodPan)


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
doc = example.provenance()
##print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
