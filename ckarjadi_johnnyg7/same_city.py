import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from city_scripts import *

class same_city(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = []
    writes = ['ckarjadi_johnnyg7.commGarden_foodEstabl']  

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        commGarden = get_Col("commGardens", repo)
        foodEstabl = get_Col("foodEstabl", repo)
        
        master = count_city(commGarden,'area')
        count_gardens = master[0]
        count_cities = master[1]
##        print(count_gardens)
##        print(count_cities)
        fast_food= ['Burger King', "McDonald's","Dunkin' Donuts"]
        master = count_food(foodEstabl,'num_fast_food',count_cities,fast_food,\
                            count_gardens)
        
##        
              
        
        repo.dropPermanent("commGarden_foodEstabl")
        repo.createPermanent("commGarden_foodEstabl")
        repo['ckarjadi_johnnyg7.commGarden_foodEstabl'].insert_many(master)

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
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        #<ckarjadi_johnnyg7>#<somefile_name>
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:same_city', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:commGarden_foodEstabl', {'prov:label':'Community Gardens, Active Food Establishment Licenses', \
            prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
        get_CGFE = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_CGFE, this_script)
        doc.usage(get_CGFE, resource1, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'???'
                }
            )

        CGFE = doc.entity('dat:ckarjadi_johnnyg7#FPH', {prov.model.PROV_LABEL:'Community Gardens, Active Food Establishment Licenses', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(CGFE, this_script)
        doc.wasGeneratedBy(CGFE, get_CGFE, endTime)
        doc.wasDerivedFrom(CGFE, resource1, get_CGFE, get_CGFE, get_CGFE)
        repo.record(doc.serialize())
        repo.logout()
        return doc

same_city.execute()
doc = same_city.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))


