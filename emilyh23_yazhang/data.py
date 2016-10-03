import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from pandas import Series, DataFrame
import csv
import requests
from contextlib import closing
from urllib import request

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
        repo['alice_bob.fireHydrants'].insert_one(r1)
        '''
        # fetching the csv 
        response = request.urlopen('http://bostonopendata.boston.opendata.arcgis.com/datasets/1b0717d5b4654882ae36adc4a20fd64b_0.csv')
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
        repo['alice_bob.fireBoxes'].insert_one(r2)

        '''
        # fetching the csv 
        response = request.urlopen('http://bostonopendata.boston.opendata.arcgis.com/datasets/3a0f4db1e63a4a98a456fdb71dc37a81_4.csv')
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
        

        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/092857c15cbb49e8b214ca5e228317a1_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r3 = json.loads(response)
        s = json.dumps(r3, sort_keys=True, indent=2)
        repo.dropPermanent("fireDepartments")
        repo.createPermanent("fireDepartments")
        repo['alice_bob.fireDepartments'].insert_one(r3)
        
        '''             
        filen = '../data/fire_departments.json'
        res = open(filen, 'r')
        r3 = json.load(res)
        repo.dropPermanent("fireDepartments")
        repo.createPermanent("fireDepartments")
        repo['emilyh23_yazhang.fireDepartments'].insert_many(r3)
        '''

        url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/bffebec4fa844e84917e0f13937ec0d7_3.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r4 = json.loads(response)
        s = json.dumps(r4, sort_keys=True, indent=2)
        repo.dropPermanent("fireDistricts")
        repo.createPermanent("fireDistricts")
        repo['alice_bob.fireDistricts'].insert_one(r4)

        '''            
        filen = '../data/fire_districts.json'
        res = open(filen, 'r')
        r4 = json.load(res)
        repo.dropPermanent("fireDistricts")
        repo.createPermanent("fireDistricts")
        repo['emilyh23_yazhang.fireDistricts'].insert_many(r4)
        '''
        
        # fetching the csv 
        response = request.urlopen('https://data.cityofboston.gov/api/views/m3cw-fv46/rows.csv?accessType=DOWNLOAD&bom=true')
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/dataset') # boston open data

        this_script = doc.agent('alg:data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    
        resource = doc.entity('bod:wc8w-nujj', {'prov:label':'data', prov.model.PROV_TYPE:'bod:DataResource', 'bod:Extension':'json'})
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation', 'ont:Query':'?accessType=DOWNLOAD'})
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)
        
        fireDistricts = doc.entity('dat:fireDistricts', {prov.model.PROV_LABEL:'fireDistricts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fireDistricts, this_script)
        doc.wasGeneratedBy(fireDistricts, this_run, endTime)
        doc.wasDerivedFrom(fireDistricts, resource, this_run, this_run, this_run)
        
        fireDepartments = doc.entity('dat:fireDepartments', {prov.model.PROV_LABEL:'fireDepartments', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fireDepartments, this_script)
        doc.wasGeneratedBy(fireDepartments, this_run, endTime)
        doc.wasDerivedFrom(fireDepartments, resource, this_run, this_run, this_run)
        
        fireBoxes = doc.entity('dat:fireBoxes', {prov.model.PROV_LABEL:'fireBoxes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fireBoxes, this_script)
        doc.wasGeneratedBy(fireBoxes, this_run, endTime)
        doc.wasDerivedFrom(fireBoxes, resource, this_run, this_run, this_run)  
        
        fireHydrants = doc.entity('dat:fireHydrants', {prov.model.PROV_LABEL:'fireHydrants', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fireHydrants, this_script)
        doc.wasGeneratedBy(fireHydrants, this_run, endTime)
        doc.wasDerivedFrom(fireHydrants, resource, this_run, this_run, this_run)   
        
        Fire_311_Service_Requests = doc.entity('dat:Fire_311_Service_Requests', {prov.model.PROV_LABEL:'Fire_311_Service_Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Fire_311_Service_Requests, this_script)
        doc.wasGeneratedBy(Fire_311_Service_Requests, this_run, endTime)
        doc.wasDerivedFrom(Fire_311_Service_Requests, resource, this_run, this_run, this_run)  
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof