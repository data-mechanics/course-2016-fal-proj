import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv

class example(dml.Algorithm):
    contributor = 'emilyh23_yazhang'
    reads = []
    writes = ['emilyh23_yazhang.Fire_311_Service_Requests', 'emilyh23_yazhang.fireBoxes', 'emilyh23_yazhang.fireDepartments', 'emilyh23_yazhang.fireDistricts', 'emilyh23_yazhang.fireHydrants']    
        
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emilyh23_yazhang', 'emilyh23_yazhang')

        '''
        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/1b0717d5b4654882ae36adc4a20fd64b_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r1 = json.loads(response)
        s = json.dumps(r1, sort_keys=True, indent=2)
        repo.dropPermanent("fireHydrants")
        repo.createPermanent("fireHydrants")
        repo['emilyh23_yazhang.fireHydrants'].insert_one(r1)
        '''
        
        ### fireHydrant data from CSV converted to a cleaner version than the normal geojson file ###
        # fetching the csv 
        response = urllib.request.urlopen('http://bostonopendata.boston.opendata.arcgis.com/datasets/1b0717d5b4654882ae36adc4a20fd64b_0.csv')
        data = response.read()
        # Save the string to a file
        csvstr = str(data).strip("b'")

        # saving the csv to data folder
        lines = csvstr.split("\\n")
        d = open("../data/fire_hydrant_new.csv", "w")
        for line in lines:
            d.write(line + "\n")
                    
        # parsing the new csv to get json file
        f=open("../data/fire_hydrant_new.csv", 'r')
        csv_reader1 = csv.DictReader(f)
        
        json_filename_1 = "../data/fire_hydrant_new.json"
        jsonWriter_1 = open(json_filename_1,'w') 
        dataJson = "[\n\t" + ",\n\t".join([json.dumps(row) for row in csv_reader1]) + "\n]"
        jsonWriter_1.write(dataJson)
        jsonWriter_1.close()        
        
        filen = '../data/fire_hydrant_new.json'
        res = open(filen, 'r')
        r1 = json.load(res)
        repo.dropPermanent("fireHydrants")
        repo.createPermanent("fireHydrants")
        repo['emilyh23_yazhang.fireHydrants'].insert_many(r1)

        '''
        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/3a0f4db1e63a4a98a456fdb71dc37a81_4.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r2 = json.loads(response)
        s = json.dumps(r2, sort_keys=True, indent=2)
        repo.dropPermanent("fireBoxes")
        repo.createPermanent("fireBoxes")
        repo['emilyh23_yazhang.fireBoxes'].insert_one(r2)
        '''
        
        ### fireBoxess data from CSV converted to a cleaner version than the normal geojson file ###
        # fetching the csv 
        response = urllib.request.urlopen('http://bostonopendata.boston.opendata.arcgis.com/datasets/3a0f4db1e63a4a98a456fdb71dc37a81_4.csv')
        data = response.read()
        # Save the string to a file
        csvstr = str(data).strip("b'")

        # saving the csv to data folder
        lines = csvstr.split("\\n")
        d2 = open("../data/fire_boxes_new.csv", "w")
        for line in lines:
            d2.write(line + "\n")
                    
        # parsing the new csv to get json file
        f2=open("../data/fire_boxes_new.csv", 'r')
        csv_reader2 = csv.DictReader(f2)
        
        json_filename_2 = "../data/fire_boxes_new.json"
        jsonWriter_2 = open(json_filename_2,'w') 
        dataJson_2 = "[\n\t" + ",\n\t".join([json.dumps(row) for row in csv_reader2]) + "\n]"
        jsonWriter_2.write(dataJson_2)
        jsonWriter_2.close()        
        
        filen = '../data/fire_boxes_new.json'
        res = open(filen, 'r')
        r2 = json.load(res)
        repo.dropPermanent("fireBoxes")
        repo.createPermanent("fireBoxes")
        repo['emilyh23_yazhang.fireBoxes'].insert_many(r2)

        #fireDepartments dataset
        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/092857c15cbb49e8b214ca5e228317a1_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r3 = json.loads(response)
        s = json.dumps(r3, sort_keys=True, indent=2)
        repo.dropPermanent("fireDepartments")
        repo.createPermanent("fireDepartments")
        repo['emilyh23_yazhang.fireDepartments'].insert_one(r3)

        #fireDistricts dataset
        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/bffebec4fa844e84917e0f13937ec0d7_3.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("fireDistricts")
        repo.createPermanent("fireDistricts")
        repo['emilyh23_yazhang.fireDistricts'].insert_one(r)

        ### 311 Service Requests data from CSV converted to a cleaner version than the normal geojson file ###
        # fetching the csv 
        response = urllib.request.urlopen('https://data.cityofboston.gov/api/views/m3cw-fv46/rows.csv?accessType=DOWNLOAD&bom=true')
        data = response.read()
        # Save the string to a file
        csvstr = str(data).strip("b'")

        # saving the csv to data folder
        lines = csvstr.split("\\n")
        d5 = open("../data/Fire_311_Requests_new.csv", "w")
        for line in lines:
            d5.write(line + "\n")
                    
        # parsing the new csv to get json file
        f5=open("../data/Fire_311_Requests_new.csv", 'r')
        csv_reader5 = csv.DictReader(f5)
        
        json_filename_5 = "../data/Fire_311_Requests_new.json"
        jsonWriter_5 = open(json_filename_5,'w') 
        dataJson_5 = "[\n\t" + ",\n\t".join([json.dumps(row) for row in csv_reader5]) + "\n]"
        jsonWriter_5.write(dataJson_5)
        jsonWriter_5.close()   
        
        filen = '../data/Fire_311_Requests_new.json'
        res = open(filen, 'r')
        r5 = json.load(res)
        repo.dropPermanent("Fire_311_Service_Requests")
        repo.createPermanent("Fire_311_Service_Requests")
        repo['emilyh23_yazhang.Fire_311_Service_Requests'].insert_many(r5)  
    
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
        repo.authenticate('emilyh23_yazhang', 'emilyh23_yazhang')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/emilyh23_yazhang') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/emilyh23_yazhang') # The data sets are in <user>#<collection> format.
        
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        #doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/api/views/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets') # boston open data

        this_script = doc.agent('alg:emilyh23_yazhang#data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        #get fireDistricts dataset
        get_fireDistricts = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get_fireDistricts', prov.model.PROV_TYPE:'ont:Retrieval'})
        datasets_fireDistricts = doc.entity('bod:bffebec4fa844e84917e0f13937ec0d7_3', {'prov:label':'fireDistricts', prov.model.PROV_TYPE:'ont:DataResource', 'bod:Extension:':'geojson'})
        doc.wasAssociatedWith(get_fireDistricts, this_script)
        doc.used(get_fireDistricts, datasets_fireDistricts, startTime)

        fireDistricts = doc.entity('dat:emilyh23_yazhang#fireDistricts', {prov.model.PROV_LABEL:'fireDistricts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fireDistricts, this_script)
        doc.wasGeneratedBy(fireDistricts, get_fireDistricts, endTime)
        doc.wasDerivedFrom(fireDistricts, datasets_fireDistricts, get_fireDistricts, get_fireDistricts, get_fireDistricts)

        #get fireDepartments dataset
        get_fireDepartments = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get_fireDepartments', prov.model.PROV_TYPE:'ont:Retrieval'})
        datasets_fireDepartments = doc.entity('bod:092857c15cbb49e8b214ca5e228317a1_2', {'prov:label':'fireDepartments', prov.model.PROV_TYPE:'ont:DataResource', 'bod:Extension:':'geojson'})
        doc.wasAssociatedWith(get_fireDepartments, this_script)
        doc.used(get_fireDepartments, datasets_fireDepartments, startTime)

        fireDepartments = doc.entity('dat:emilyh23_yazhang#fireDepartments', {prov.model.PROV_LABEL:'fireDepartments', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fireDepartments, this_script)
        doc.wasGeneratedBy(fireDepartments, get_fireDepartments, endTime)
        doc.wasDerivedFrom(fireDepartments, datasets_fireDepartments, get_fireDepartments, get_fireDepartments, get_fireDepartments)

        #get fireBoxes dataset
        get_fireBoxes = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get_fireBoxes', prov.model.PROV_TYPE:'ont:Retrieval'})
        datasets_fireBoxes = doc.entity('bod:3a0f4db1e63a4a98a456fdb71dc37a81_4', {'prov:label':'fireBoxes', prov.model.PROV_TYPE:'ont:DataResource', 'bod:Extension:':'csv'})
        doc.wasAssociatedWith(get_fireBoxes, this_script)
        doc.used(get_fireBoxes, datasets_fireBoxes, startTime)

        fireBoxes = doc.entity('dat:emilyh23_yazhang#fireBoxes', {prov.model.PROV_LABEL:'fireBoxes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fireBoxes, this_script)
        doc.wasGeneratedBy(fireBoxes, get_fireBoxes, endTime)
        doc.wasDerivedFrom(fireBoxes, datasets_fireBoxes, get_fireBoxes, get_fireBoxes, get_fireBoxes)  

        #get fireHydrants dataset
        get_fireHydrants = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get_fireHyrdrants', prov.model.PROV_TYPE:'ont:Retrieval'})
        datasets_fireHydrants = doc.entity('bod:1b0717d5b4654882ae36adc4a20fd64b_0', {'prov:label':'fireHydrants', prov.model.PROV_TYPE:'ont:DataResource', 'bod:Extension:':'csv'})
        doc.wasAssociatedWith(get_fireHydrants, this_script)
        doc.used(get_fireHydrants, datasets_fireHydrants, startTime)

        fireHydrants = doc.entity('dat:emilyh23_yazhang#fireHydrants', {prov.model.PROV_LABEL:'fireHydrants', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fireHydrants, this_script)
        doc.wasGeneratedBy(fireHydrants, get_fireHydrants, endTime)
        doc.wasDerivedFrom(fireHydrants, datasets_fireHydrants, get_fireHydrants, get_fireHydrants, get_fireHydrants)   

        #get 311 Service Requests dataset
        get_311 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get_311', prov.model.PROV_TYPE:'ont:Retrieval'})
        resource = doc.entity('bdp:m3cw-fv46/rows', {'prov:label':'Fire_311_Service_Requests', prov.model.PROV_TYPE:'ont:DataResource', 'bdp:Extension:':'csv', 'ont:Query': '?accessType=DOWNLOAD&bom=true'})
        doc.wasAssociatedWith(get_311, this_script)
        doc.used(get_311, resource, startTime)
        
        Fire_311_Service_Requests = doc.entity('dat:emilyh23_yazhang#Fire_311_Service_Requests', {prov.model.PROV_LABEL:'Fire_311_Service_Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Fire_311_Service_Requests, this_script)
        doc.wasGeneratedBy(Fire_311_Service_Requests, get_311, endTime)
        doc.wasDerivedFrom(Fire_311_Service_Requests, resource, get_311, get_311, get_311)  
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
