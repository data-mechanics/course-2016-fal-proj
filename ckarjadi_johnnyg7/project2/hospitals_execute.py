import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from hospitals_scripts import *

class hospitals(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = []
    writes = ['ckarjadi_johnnyg7.new_Hospitals']  

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        h_Locs = get_Col("h_Locs", repo)
        c_Inc = get_Col("c_Inc", repo)
        w_Jams = get_Col("w_Jams",repo)

        change = change_hospitals(h_Locs)
        crimes_close = closest_crimes(change,c_Inc)
        
        jams_close = closest_jams(crimes_close,w_Jams)
        change = change_cluster(jams_close)
        # change = (lat, long, num_crimes+num_jams)
   
        n = 3
        # n = number of hospitals to add
        ini_means = create_means(change,n)
        hosp_locations = k_means(ini_means,change)

        final = change_k_means(hosp_locations)
        
        repo.dropPermanent("new_Hospitals")
        repo.createPermanent("new_Hospitals")
        repo['ckarjadi_johnnyg7.new_Hospitals'].insert_many(final)
        
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

        this_script = doc.agent('alg:hospital_scripts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:new_Hospitals', {'prov:label':'Community Gardens, Active Food Establishment Licenses', \
            prov.model.PROV_TYPE:'ont:DataResource'})
        get_h_tj = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_h_tj, this_script)
        doc.usage(get_h_tj, resource1, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )

        h_tj = doc.entity('dat:ckarjadi_johnnyg7#h_tj', {prov.model.PROV_LABEL:'Hospitals and Traffic Jams', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(h_tj, this_script)
        doc.wasGeneratedBy(h_tj, get_h_tj, endTime)
        doc.wasDerivedFrom(h_tj, resource1, get_h_tj, get_h_tj, get_h_tj)
        repo.record(doc.serialize())
        repo.logout()
        return doc

hospitals.execute()
doc = hospitals.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))


