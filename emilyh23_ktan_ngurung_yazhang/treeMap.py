import json
import dml
import prov.model
import datetime
import uuid
import geocoder
from collections import Counter
import pandas as pd
import numpy as np 
from bs4 import BeautifulSoup
import urllib.request
import re
import itertools
import collections

class treeMap(dml.Algorithm):
    contributor = 'emilyh23_ktan_ngurung_yazhang'
    reads = ['emilyh23_ktan_ngurung_yazhang.zipcodeRatings']
    writes = ['emilyh23_ktan_ngurung_yazhang.treeMap']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emilyh23_ktan_ngurung_yazhang', 'emilyh23_ktan_ngurung_yazhang')

        # Get zipcodeRatings data
        zipcodeRatings = repo.emilyh23_ktan_ngurung_yazhang.zipcodeRatings.find_one()
        zipcodeRatings.pop('_id', None)

        # create json for tree map configuration
        rating_3 = []
        rating_2 = []
        rating_1 = []
        
        for zc in zipcodeRatings:
            zc_data = zipcodeRatings[zc]
            zc_data["name"] = zc
            
            # value determines size of box in tree-map
            zc_data["value"] = zc_data.pop("area")
            
            # rate determines color of box in tree-map (dark -> greater value)
            zc_data["rate"] = zc_data.pop("population_density")
            if (zc_data["overall_star"]) == 3:
                zc_data.pop("overall_star")
                rating_3.append(zc_data)
        
            elif(zc_data["overall_star"]) == 2:
                zc_data.pop("overall_star")
                rating_2.append(zc_data)
            else:
                zc_data.pop("overall_star")
                rating_1.append(zc_data)

        treeMapDict = {"children": [{"children": rating_1, "rate": 1, "name": "rating 1"}, {"children": rating_2, "rate": 2, "name": "rating 2"}, {"children": rating_3, "rate": 3, "name": "rating 3"}], "rate": -0.28656039777712783, "name": "Boston zipcode ratings"}
       
       # Convert dictionary into JSON object 
        data = json.dumps(treeMapDict, sort_keys=True, indent=2)
        r = json.loads(data)

        # Create new dataset called zipcodeRatings
        repo.dropPermanent("treeMap")
        repo.createPermanent("treeMap")
        repo['emilyh23_ktan_ngurung_yazhang.treeMap'].insert_one(r)

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
        repo.authenticate('emilyh23_ktan_ngurung_yazhang', 'emilyh23_ktan_ngurung_yazhang')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:emilyh23_ktan_ngurung_yazhang#treeMap', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        zipcode_ratings_resource = doc.entity('dat:emilyh23_ktan_ngurung_yazhang/zipcoderatings', {'prov:label':'Rating of Each Category and the Overall Rating for each Zipcode', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, zipcode_ratings_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )      
        treeMap = doc.entity('dat:emilyh23_ktan_ngurung_yazhang#treeMap', {prov.model.PROV_LABEL:'Data for tree map configuration', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(treeMap, this_script)
        doc.wasGeneratedBy(treeMap, this_run, endTime)
        doc.wasDerivedFrom(treeMap, zipcode_ratings_resource, this_run, this_run, this_run)
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

treeMap.execute() 
doc = treeMap.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
