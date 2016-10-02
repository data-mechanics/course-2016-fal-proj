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
        '''
        
        # takes latitude,longtitude
        url = 'https://maps.googleapis.com/maps/api/distancematrix/json?units=imperial&origins=42.350093449464815,-71.049703346416152&destinations=42.349979362118951,-71.049453168719367&key=AIzaSyAqIP1PGmbNC3Plc7YfDbR7zb60tzTBtBA'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        
        # testing for google api parsing later
        filen = '../data/food_estab_test.json'
        res = open(filen, 'r')
        r = json.load(res)
        
        # r_loc gets all the location values for the key 'location' in r
        r_loc = [k['location'] for k in r]  
        # cleaned up r_loc, r_loc format: [{'longitude': '...', 'latitude': '...'}, {'longitude': '...', 'latitude': '...'}, ...]
        for dic in r_loc:
            dic['latitude'] = dic.pop('_latitude')
            dic['longitude'] = dic.pop('_longitude')
            del dic['_needs_recoding']
        print(r_loc)

        #repo.dropPermanent("foodEst")
        #repo.createPermanent("foodEst")
        #repo['emilyh23_yazhang.foodEst'].insert_one(r)  
        # testing ends        
        
        # r is a list        
        r = json.loads(response)        
        # s is a string
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("distances")
        repo.createPermanent("distances")
        repo['emilyh23_yazhang.distances'].insert_one(r) 

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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/') # boston open data
        doc.add_namespace('gma', 'https://developers.google.com/maps/') # google maps api
        
        #this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        this_script = doc.agent('alg:distances', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    
        resource = doc.entity('gma:wc8w-nujj', {'prov:label':'distances', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation', 'ont:Query':'?accessType=NONE'})        
        #get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        #get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)
        
        '''
        doc.usage(this_run, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                }
            )

        
        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)
        '''
        
        distances = doc.entity('dat:distances', {prov.model.PROV_LABEL:'distances', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(distances, this_script)
        doc.wasGeneratedBy(distances, this_run, endTime)
        doc.wasDerivedFrom(distances, resource, this_run, this_run, this_run)
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