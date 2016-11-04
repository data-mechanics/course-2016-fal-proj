import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = []
    writes = ['ckarjadi_johnnyg7.propVal', 'ckarjadi_johnnyg7.foodPan','ckarjadi_johnnyg7.Hospitals','ckarjadi_johnnyg7.Active_Work_Zones']
    writes +=['ckarjadi_johnnyg7.Waze_Jams', 'ckarjadi_johnnyg7.foodEstabl','ckarjadi_johnnyg7.commGardens.']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        
        url ='https://data.cityofboston.gov/resource/n7za-nsjh.json?$where=gross_tax%20%3E%200&$select=zipcode,gross_tax'
        #url = 'https://data.cityofboston.gov/resource/n7za-nsjh.json?$limit=20&$where=gross_tax%20%3E%200'
        #only need 'zipcode' and 'grosstax'
        #url = 'https://data.cityofboston.gov/resource/n7za-nsjh.json?$where=gross_tax%20%3E%200'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print(s)
        repo.dropPermanent("propVal")
        repo.createPermanent("propVal")
        repo['ckarjadi_johnnyg7.propVal'].insert_many(r)

        url='https://data.cityofboston.gov/resource/4tie-bhxw.json?$select=zip_code'
        #url = 'https://data.cityofboston.gov/resource/4tie-bhxw.json?$limit=20'
        #url = 'https://data.cityofboston.gov/resource/4tie-bhxw.json?'
        #only need zip_code
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print(s)
        repo.dropPermanent("foodPan")
        repo.createPermanent("foodPan")
        repo['ckarjadi_johnnyg7.foodPan'].insert_many(r)

        url='https://data.cityofboston.gov/resource/u6fv-m8v4.json?$select=location_zip'
        #url='https://data.cityofboston.gov/resource/u6fv-m8v4.json?$limit=20'
        #url='https://data.cityofboston.gov/resource/u6fv-m8v4.json?'
        #only need location_zip
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print(s)
        repo.dropPermanent("Hospitals")
        repo.createPermanent("Hospitals")
        repo['ckarjadi_johnnyg7.Hospitals'].insert_many(r)

        url='https://data.cityofboston.gov/resource/hx38-wur4.json?$select=street'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print(s)
        repo.dropPermanent("Active_Work_Zones")
        repo.createPermanent("Active_Work_Zones")
        repo['ckarjadi_johnnyg7.Active_Work_Zones'].insert_many(r)

        url='https://data.cityofboston.gov/resource/dih6-az4h.json?$where=delay%20%3E%200&$select=street,delay'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print(s)
        repo.dropPermanent("Waze_Jams")
        repo.createPermanent("Waze_Jams")
        repo['ckarjadi_johnnyg7.Waze_Jams'].insert_many(r)
##
        url='https://data.cityofboston.gov/resource/fdxy-gydq.json?$select=businessname,city'
        #url='https://data.cityofboston.gov/resource/fdxy-gydq.json?$limit=20'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print(s)
        repo.dropPermanent("foodEstabl")
        repo.createPermanent("foodEstabl")
        repo['ckarjadi_johnnyg7.foodEstabl'].insert_many(r)
        url='https://data.cityofboston.gov/resource/rdqf-ter7.json?$select=area'
        #url='https://data.cityofboston.gov/resource/rdqf-ter7.json?$limit=20'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        #print(s)
        repo.dropPermanent("commGardens")
        repo.createPermanent("commGardens")
        repo['ckarjadi_johnnyg7.commGardens'].insert_many(r)
        
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

        this_script = doc.agent('alg:ckarjadi_johnnyg7#retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_propVal = doc.entity('bdp:n7za-nsjh', {'prov:label':'Property Values 2016', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_foodPan = doc.entity('bdp:4tie-bhxw', {'prov:label':'Food Pantries', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_Hospitals = doc.entity('bdp:u6fv-m8v4', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        resource_Active_Work_Zones = doc.entity('bdp:hx38-wur4', {'prov:label':'Active_Work_Zones', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_Waze_Jams = doc.entity('bdp:dih6-az4h', {'prov:label':'Waze_Jams', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        resource_foodEstabl = doc.entity('bdp:fdxy-gydq', {'prov:label':'Food Establishments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_commGardens = doc.entity('bdp:rdqf-ter7', {'prov:label':'Community Gardens', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension:':'json'})


        get_propVal = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_foodPan = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_Hospitals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        get_Active_Work_Zones = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_Waze_Jams = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        get_foodEstabl = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_commGardens = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_propVal, this_script)
        doc.wasAssociatedWith(get_foodPan, this_script)
        doc.wasAssociatedWith(get_Hospitals, this_script)

        doc.wasAssociatedWith(get_Active_Work_Zones, this_script)
        doc.wasAssociatedWith(get_Waze_Jams,this_script)

        doc.wasAssociatedWith(get_foodEstabl, this_script)
        doc.wasAssociatedWith(get_commGardens, this_script) 
        doc.usage(get_propVal, resource_propVal, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'$where=gross_tax%20%3E%200&$select=zipcode,gross_tax'
                }
            )
        doc.usage(get_foodPan, resource_foodPan, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'$select=zip_code'
                }
            )
        doc.usage(get_Hospitals, resource_Hospitals, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'$select=location_zip'
                }
            )
        doc.usage(get_Active_Work_Zones, resource_Active_Work_Zones, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'???'
                }
            )
        doc.usage(get_Waze_Jams, resource_Waze_Jams, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'$where=delay%20%3E%200'
                }
            )
        doc.usage(get_foodEstabl, resource_foodEstabl, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:QUERY':'???'
                }
            )
        doc.usage(get_commGardens, resource_commGardens, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:QUERY':'???'
                }
            )

        prop_Val = doc.entity('dat:ckarjadi_johnnyg7#prop_Val', {prov.model.PROV_LABEL:'Property Value 2016', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(prop_Val, this_script)
        doc.wasGeneratedBy(prop_Val, get_propVal, endTime)
        doc.wasDerivedFrom(prop_Val, resource_propVal, get_propVal, get_propVal, get_propVal)
        
        foodPan = doc.entity('dat:ckarjadi_johnnyg7#foodPan', {prov.model.PROV_LABEL:'Food Pantries', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(foodPan, this_script)
        doc.wasGeneratedBy(foodPan, get_foodPan, endTime)
        doc.wasDerivedFrom(foodPan, resource_foodPan, get_foodPan, get_foodPan, get_foodPan)

        Hospitals = doc.entity('dat:ckarjadi_johnnyg7#Hospitals', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Hospitals, this_script)
        doc.wasGeneratedBy(Hospitals, get_Hospitals, endTime)
        doc.wasDerivedFrom(Hospitals, resource_Hospitals, get_Hospitals, get_Hospitals, get_Hospitals)
        
        Active_Work_Zones = doc.entity('dat:ckarjadi_johnnyg7#Active_Work_Zones', {prov.model.PROV_LABEL:'Active_Work_Zones', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Active_Work_Zones, this_script)
        doc.wasGeneratedBy(Active_Work_Zones, get_Active_Work_Zones, endTime)
        doc.wasDerivedFrom(Active_Work_Zones, resource_Active_Work_Zones, get_Active_Work_Zones, get_Active_Work_Zones, get_Active_Work_Zones)
        
        Waze_Jams = doc.entity('dat:ckarjadi_johnnyg7#Waze_Jams', {prov.model.PROV_LABEL:'Waze_Jams', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Waze_Jams, this_script)
        doc.wasGeneratedBy(Waze_Jams, get_Waze_Jams, endTime)
        doc.wasDerivedFrom(Waze_Jams, resource_Waze_Jams, get_Waze_Jams, get_Waze_Jams, get_Waze_Jams)

        foodEstabl = doc.entity('dat:ckarjadi_johnnyg7#foodEstabl', {prov.model.PROV_LABEL:'Food Establishments', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(foodEstabl, this_script)
        doc.wasGeneratedBy(foodEstabl, get_foodEstabl, endTime)
        doc.wasDerivedFrom(foodEstabl, resource_foodEstabl, get_foodEstabl, get_foodEstabl, get_foodEstabl)

        commGardens = doc.entity('dat:ckarjadi_johnnyg7#commGardens', {prov.model.PROV_LABEL:'Community Gardens', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(commGardens, this_script)
        doc.wasGeneratedBy(commGardens, get_commGardens, endTime)
        doc.wasDerivedFrom(commGardens, resource_commGardens, get_commGardens, get_commGardens, get_commGardens)  
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
##doc = example.provenance()
####print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
