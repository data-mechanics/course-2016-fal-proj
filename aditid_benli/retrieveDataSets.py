import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import vincenty

class example(dml.Algorithm):
    contributor = 'aditid_benli'
    reads = []
    writes = ['aditid_benli.jam', 'aditid_benli.comparking', 'aditid_benli.inters', 'aditid_benli.metparking', 'aditid_benli.partickets','aditid_benli.crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli', 'aditid_benli')
        
        url = 'https://data.cityofboston.gov/resource/dih6-az4h.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("jam")
        repo.createPermanent("jam")
        repo['aditid_benli.jam'].insert_many(r)
        
        url = 'https://data.cambridgema.gov/resource/vr3p-e9ke.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("comparking")
        repo.createPermanent("comparking")
        repo['aditid_benli.comparking'].insert_many(r)

        url = 'https://data.cambridgema.gov/resource/impv-6fac.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("inters")
        repo.createPermanent("inters")
        repo['aditid_benli.inters'].insert_many(r)
        
        url = 'https://data.cambridgema.gov/resource/up94-ihbw.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("metparking")
        repo.createPermanent("metparking")
        repo['aditid_benli.metparking'].insert_many(r)
        
        url = 'https://data.cambridgema.gov/resource/m4i2-83v6.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("partickets")
        repo.createPermanent("partickets")
        repo['aditid_benli.partickets'].insert_many(r)
        
        url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("crime")
        repo.createPermanent("crime")
        repo['aditid_benli.crime'].insert_many(r)

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
        repo.authenticate('aditid_benli', 'aditid_benli')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aditid_benli#retrieveDataSets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        

        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        

        jamResource = doc.entity('bdp:dih6-az4h', {'prov:label':'Waze Jame Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getJam = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getJam, this_script)
        doc.usage(getJam, jamResource, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )
 

        comparkingResource = doc.entity('bdp:vr3p-e9ke', {'prov:label':'Commerical Parking Map', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getComparking = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getComparking, this_script)
        doc.usage(getComparking, comparkingResource, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )
 

        intersResource = doc.entity('bdp:impv-6fac', {'prov:label':'Intersections of Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getInters = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getInters, this_script)
        doc.usage( getInters, intersResource, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )
 

        metparkingResource = doc.entity('bdp:up94-ihbw', {'prov:label':'Metered Parking Lots', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getMetparking = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getMetparking, this_script)
        doc.usage(getMetparking, metparkingResource, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )
 

        particketsResource = doc.entity('bdp:m4i2-83v6', {'prov:label':'Cambridge Parking Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getPartickets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getPartickets, this_script)
        doc.usage(getPartickets, particketsResource, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )
 

        crimeResource = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime in Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getCrime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getCrime, this_script)
        doc.usage( getCrime, crimeResource, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )

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

        jam = doc.entity('dat:aditid_benli#jam', {prov.model.PROV_LABEL:'Waze jam data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(jam, this_script)
        doc.wasGeneratedBy(jam, getJam, endTime)
        doc.wasDerivedFrom(jam, resource, getJam, getJam, getJam)

        
        comparking = doc.entity('dat:aditid_benli#comparking', {prov.model.PROV_LABEL:'Commerical parking Spaces', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(comparking, this_script)
        doc.wasGeneratedBy(comparking, get, endTime)
        doc.wasDerivedFrom(comparking, resource, getComparking, getComparking, getComparking)

        inters = doc.entity('dat:aditid_benli#inters', {prov.model.PROV_LABEL:'Intersections', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(inters, this_script)
        doc.wasGeneratedBy(inters, getInters, endTime)
        doc.wasDerivedFrom(inters, resource, getInters, getInters, getInters)

        metparking = doc.entity('dat:aditid_benli#metparking', {prov.model.PROV_LABEL:'Metered Parking', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(metparking, this_script)
        doc.wasGeneratedBy(metparking, getMetparking, endTime)
        doc.wasDerivedFrom(metparking, resource, getMetparking, getMetparking, getMetparking)

        partickets = doc.entity('dat:aditid_benli#partickets', {prov.model.PROV_LABEL:'Parking Tickets', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(partickets, this_script)
        doc.wasGeneratedBy(partickets, getPartickets, endTime)
        doc.wasDerivedFrom(partickets, resource, getPartickets, getPartickets, getPartickets)

        crime = doc.entity('dat:aditid_benli#crime', {prov.model.PROV_LABEL:'Crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, getCrime, endTime)
        doc.wasDerivedFrom(crime, resource, getCrime, getCrime, getCrime)

         

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof