import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.son import SON
from bson.code import Code

class retrieve(dml.Algorithm):
    contributor = 'dichgao'
    reads = []
    writes = ['resource']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('dichgao', 'dichgao')
        
        '''
        url = 'https://data.cityofboston.gov/resource/cp2t-tvxx.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("trash_schedule")
        repo.createPermanent("trash_schedule")
        repo['dichgao.trash_schedule'].insert_many(r)
        '''

        url = 'https://data.cityofboston.gov/resource/wisd-cxpy.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("street_sweeping_schedule")
        repo.createPermanent("street_sweeping_schedule")
        repo['dichgao.street_sweeping_schedule'].insert_many(r)


        url = 'https://data.cityofboston.gov/resource/nybq-xu5r.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("big_belly_alerts")
        repo.createPermanent("big_belly_alerts")
        repo['dichgao.big_belly_alerts'].insert_many(r)
        
           
        url = 'https://data.cityofboston.gov/resource/jbcd-dknd.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("mayors_24_hour_hotline")
        repo.createPermanent("mayors_24_hour_hotline")
        repo['dichgao.mayors_24_hour_hotline'].insert_many(r)

        #The data set "311, Service Requests" was retrieved successfully at 1:00 p.m.
        #on Oct 3, but failed after retrieval. 
        '''
        url = 'https://data.cityofboston.gov/resource/wc8w-nujj.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("311")
        repo.createPermanent("311")
        repo['dichgao.311'].insert_many(r)
        '''
        #backup plan to retrieve "311, Service Requests"
        url = 'http://datamechanics.io/data/dichgao/311.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("311")
        repo.createPermanent("311")
        repo['dichgao.311'].insert_many(r)        
        
        url = 'https://data.cityofboston.gov/resource/rdqf-ter7.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("gardens")
        repo.createPermanent("gardens")
        repo['dichgao.gardens'].insert_many(r)
               
        pickup_request = []
        for data in repo['dichgao.mayors_24_hour_hotline'].find({'case_title':'Pick up Dead Animal'}):
            pickup_request.append(data)
        repo.dropPermanent('pickup_hotline')
        repo.createPermanent('pickup_hotline')
        repo['dichgao.pickup_hotline'].insert_many(pickup_request)
        
        
        pickup_request = []
        for data in repo['dichgao.311'].find({'case_title':'Pick up Dead Animal'}):
            pickup_request.append(data)
        repo.dropPermanent('pickup_311')
        repo.createPermanent('pickup_311')
        repo['dichgao.pickup_311'].insert_many(pickup_request)
        
        
        st_clean_request = []
        for data in repo['dichgao.mayors_24_hour_hotline'].find({'case_title':'Requests for Street Cleaning'}):
            st_clean_request.append(data)
        repo.dropPermanent('request_hotline')
        repo.createPermanent('request_hotline')
        repo['dichgao.request_hotline'].insert_many(st_clean_request)        

        st_clean_request = []
        for data in repo['dichgao.311'].find({'case_title':'Requests for Street Cleaning'}):
            st_clean_request.append(data)
        repo.dropPermanent('request_311')
        repo.createPermanent('request_311')
        repo['dichgao.request_311'].insert_many(st_clean_request)
        
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
        repo.authenticate('dichgao', 'dichgao')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/dichgao') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/dichgao') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:dichgao#proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource0 = doc.entity('bdp:wisd-cxpy', {'prov:label':'Street Sweeping Schedule', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource1 = doc.entity('bdp:nybq-xu5r', {'prov:label':'Big Belly Alerts 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('bdp:jbcd-dknd', {'prov:label':'Mayors 24 Hour Hotline', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource3 = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})        
        resource4 = doc.entity('bdp:rdqf-ter7', {'prov:label':'Community Gardens', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_pickup_hotline = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_pickup_311 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        get_request_hotline = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_request_311 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_pickup_hotline, this_script)
        doc.wasAssociatedWith(get_pickup_311, this_script)
        
        doc.wasAssociatedWith(get_request_hotline, this_script)
        doc.wasAssociatedWith(get_request_311, this_script)
        
        doc.usage(get_pickup_hotline, resource2, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                }
            )
        doc.usage(get_pickup_311, resource3, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                }
            )
        doc.usage(get_request_hotline, resource2, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                }
            )
        doc.usage(get_request_311, resource3, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                }
            )

        pickup_hotline = doc.entity('dat:dichgao#pickup_hotline', {prov.model.PROV_LABEL:'Pickup Hotline', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pickup_hotline, this_script)
        doc.wasGeneratedBy(pickup_hotline, get_pickup_hotline, endTime)
        doc.wasDerivedFrom(pickup_hotline, resource2, get_pickup_hotline, get_pickup_hotline, get_pickup_hotline)

        pickup_311 = doc.entity('dat:dichgao#pickup_311', {prov.model.PROV_LABEL:'Pickup 311', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pickup_311, this_script)
        doc.wasGeneratedBy(pickup_311, get_pickup_311, endTime)
        doc.wasDerivedFrom(pickup_311, resource3, get_pickup_311, get_pickup_311, get_pickup_311)

        request_hotline = doc.entity('dat:dichgao#request_hotline', {prov.model.PROV_LABEL:'Request Hotline', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(request_hotline, this_script)
        doc.wasGeneratedBy(request_hotline, get_request_hotline, endTime)
        doc.wasDerivedFrom(request_hotline, resource2, get_request_hotline, get_request_hotline, get_request_hotline)

        request_311 = doc.entity('dat:dichgao#request_311', {prov.model.PROV_LABEL:'Request 311', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(request_311, this_script)
        doc.wasGeneratedBy(request_311, get_request_311, endTime)
        doc.wasDerivedFrom(request_311, resource3, get_request_311, get_request_311, get_request_311)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

retrieve.execute()
doc = retrieve.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
