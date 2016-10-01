import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    #contributor = 'alice_bob'
    #reads = []
    #writes = ['alice_bob.lost', 'alice_bob.found']
    
    contributor = 'emilyh23_yazhang'
    reads = []
    writes = ['emilyh23_yazhang.lost', 'emilyh23_yazhang.found']    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        #repo.authenticate('alice_bob', 'alice_bob')
        repo.authenticate('emilyh23_yazhang', 'emilyh23_yazhang')
        
        '''
        url = 'http://cs-people.bu.edu/lapets/591/examples/lost.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("lost")
        repo.createPermanent("lost")
        repo['emilyh23_yazhang.lost'].insert_many(r)
        
        url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("found")
        repo.createPermanent("found")
        repo['emilyh23_yazhang.found'].insert_many(r)
                     
        
        filen = '../data/food_estab.json'
        res = open(filen, 'r')
        r = json.load(res)
        repo.dropPermanent("foodEst")
        repo.createPermanent("foodEst")
        repo['emilyh23_yazhang.foodEst'].insert_one(r)   
                
        filen = '../data/corner_stores.json'
        res = open(filen, 'r')
        r = json.load(res)
        repo.dropPermanent("cornerStore")
        repo.createPermanent("cornerStore")
        repo['emilyh23_yazhang.cornerStore'].insert_one(r)   
        
        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/962da9bb739f440ba33e746661921244_9.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("parkingMeters")
        repo.createPermanent("parkingMeters")
        repo['emilyh23_yazhang.parkingMeters'].insert_one(r)   
        '''
        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/eebd3daed05a45678894db30d9bf0cfb_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("zoningDistricts")
        repo.createPermanent("zoningDistricts")
        repo['emilyh23_yazhang.zoningDistricts'].insert_one(r)  
        
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
        #repo.authenticate('alice_bob', 'alice_bob')
        repo.authenticate('emilyh23_yazhang', 'emilyh23_yazhang')
        
        #doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        #doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/emilyh23_yazhang') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/emilyh23_yazhang') # The data sets are in <user>#<collection> format.
        
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        #doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/') # boston open data

        #this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        this_script = doc.agent('alg:zoningDistricts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    
        resource = doc.entity('bod:wc8w-nujj', {'prov:label':'Zoning Districts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                }
            )
        doc.usage(get_lost, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                }
            )

        '''
        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)
        
        
        foodEst = doc.entity('dat:foodEst', {prov.model.PROV_LABEL:'foodEst', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(foodEst, this_script)
        doc.wasGeneratedBy(foodEst, get_lost, endTime)
        doc.wasDerivedFrom(foodEst, resource, get_lost, get_lost, get_lost)
        #doc.wasGeneratedBy(foodEst, this_run, endTime)
        #doc.wasDerivedFrom(foodEst, resource, this_run, this_run, this_run) # dont delete need for later
        
        cornerStore = doc.entity('dat:cornerStore', {prov.model.PROV_LABEL:'cornerStore', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cornerStore, this_script)
        doc.wasGeneratedBy(cornerStore, get_lost, endTime)
        doc.wasDerivedFrom(cornerStore, resource, get_lost, get_lost, get_lost)
        #doc.wasGeneratedBy(cornerStore, this_run, endTime)
        #doc.wasDerivedFrom(cornerStore, resource, this_run, this_run, this_run) # dont delete need for later
        
        parkingMeters = doc.entity('dat:parkingMeters', {prov.model.PROV_LABEL:'parkingMeters', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(parkingMeters, this_script)
        doc.wasGeneratedBy(parkingMeters, get_lost, endTime)
        doc.wasDerivedFrom(parkingMeters, resource, get_lost, get_lost, get_lost)
        #doc.wasGeneratedBy(parkingMeters, this_run, endTime)
        #doc.wasDerivedFrom(parkingMeters, resource, this_run, this_run, this_run) # dont delete need for later
        '''
        
        zoningDistricts = doc.entity('dat:zoningDistricts', {prov.model.PROV_LABEL:'zoningDistricts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(zoningDistricts, this_script)
        doc.wasGeneratedBy(zoningDistricts, get_lost, endTime)
        doc.wasDerivedFrom(zoningDistricts, resource, get_lost, get_lost, get_lost)
        #doc.wasGeneratedBy(zoningDistricts, this_run, endTime)
        #doc.wasDerivedFrom(zoningDistricts, resource, this_run, this_run, this_run) # dont delete need for later
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof