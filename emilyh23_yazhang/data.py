import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from pandas import Series, DataFrame
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
        
        # http://bostonopendata.boston.opendata.arcgis.com/datasets/1b0717d5b4654882ae36adc4a20fd64b_0.csv
        #### TESTING CSV TO DATAFRAME: working ###
        data = pd.read_csv('http://bostonopendata.boston.opendata.arcgis.com/datasets/1b0717d5b4654882ae36adc4a20fd64b_0.csv')
        #print(data)
        
        ### TESTING DATAFRAME TO JSON: doesn't work ###
        x = DataFrame.to_json(data)
        s = json.dumps(x, sort_keys=True, indent=2)
        #print(s)
        
        filen = '../data/fire_hydrant.json'
        res = open(filen, 'r')
        r1 = json.load(res)
        repo.dropPermanent("fireHydrants")
        repo.createPermanent("fireHydrants")
        repo['emilyh23_yazhang.fireHydrants'].insert_many(r1)   
        
        # http://bostonopendata.boston.opendata.arcgis.com/datasets/3a0f4db1e63a4a98a456fdb71dc37a81_4.csv
        filen = '../data/fire_boxes.json'
        res = open(filen, 'r')
        r2 = json.load(res)
        repo.dropPermanent("fireBoxes")
        repo.createPermanent("fireBoxes")
        repo['emilyh23_yazhang.fireBoxes'].insert_many(r2)  
        
        # http://bostonopendata.boston.opendata.arcgis.com/datasets/092857c15cbb49e8b214ca5e228317a1_2.csv
        filen = '../data/fire_departments.json'
        res = open(filen, 'r')
        r3 = json.load(res)
        repo.dropPermanent("fireDepartments")
        repo.createPermanent("fireDepartments")
        repo['emilyh23_yazhang.fireDepartments'].insert_many(r3)
       
        # http://bostonopendata.boston.opendata.arcgis.com/datasets/bffebec4fa844e84917e0f13937ec0d7_3.csv
        filen = '../data/fire_districts.json'
        res = open(filen, 'r')
        r4 = json.load(res)
        repo.dropPermanent("fireDistricts")
        repo.createPermanent("fireDistricts")
        repo['emilyh23_yazhang.fireDistricts'].insert_many(r4)         
        #
        filen = '../data/Fire_311_Service_Requests.json'
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
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/') # boston open data

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
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
