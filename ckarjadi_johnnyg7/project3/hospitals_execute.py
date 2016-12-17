import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from hospitals_scripts import *

class hospitals(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = ['ckarjadi_johnnyg7.h_Locs','ckarjadi_johnnyg7.c_Inc']
    reads+= ['ckarjadi_johnnyg7.w_Jams']
    writes = ['ckarjadi_johnnyg7.new_Hospitals']  

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        limit = 150
        #if trial == true, limit = # of entries to grab from DB
        if trial == False:
            h_Locs = get_Col("h_Locs", repo)
            c_Inc = get_Col("c_Inc", repo)
            w_Jams = get_Col("w_Jams",repo)
        else:
            h_Locs = get_Short("h_Locs", repo,limit)
            c_Inc = get_Short("c_Inc", repo,limit)
            w_Jams = get_Short("w_Jams",repo,limit)

        change = change_hospitals(h_Locs)
        crimes_close = closest_crimes(change,c_Inc)
        
        jams_close = closest_jams(crimes_close,w_Jams)
        change = change_cluster(jams_close)
        # change = (lat, long, num_crimes+num_jams)
        n = 4
        #n = int(input("Enter number of hospitals to add: "))
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
        h_Locs_R = doc.entity('dat:h_Locs', {'prov:label':'Hospital Locations', \
            prov.model.PROV_TYPE:'ont:DataResource'})
        c_Inc_R = doc.entity('dat:c_Inc', {'prov:label':'Crime Incident Reports', \
            prov.model.PROV_TYPE:'ont:DataResource'})
        w_Jams_R = doc.entity('dat:w_Jams', {'prov:label':'Waze Jams', \
            prov.model.PROV_TYPE:'ont:DataResource'})
        
        new_Hospitals_TT = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(new_Hospitals_TT, this_script)
        
        doc.usage(new_Hospitals_TT, h_Locs_R, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )
        doc.usage(new_Hospitals_TT, c_Inc_R, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )
        doc.usage(new_Hospitals_TT, w_Jams_R, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                }
            )
        new_Hospitals = doc.entity('dat:ckarjadi_johnnyg7#new_Hospitals', {prov.model.PROV_LABEL:'New Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(new_Hospitals, this_script)
        doc.wasGeneratedBy(new_Hospitals, new_Hospitals_TT, endTime)
        doc.wasDerivedFrom(new_Hospitals, h_Locs_R, new_Hospitals_TT, new_Hospitals_TT, new_Hospitals_TT)
        repo.record(doc.serialize())
        repo.logout()
        return doc

hospitals.execute()
doc = hospitals.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))

