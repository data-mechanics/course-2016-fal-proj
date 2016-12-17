import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from avg_scripts import *

class avg_exe(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = ['ckarjadi_johnnyg7.p_Val']
    writes = ['ckarjadi_johnnyg7.avg_p_Vals']  

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        limit = 20 # if trial is true, we grab only 20 entries from DB
        if trial == False:
            p_Val = get_Col("p_Val", repo)
            
        else:
            p_Val = get_Short("p_Val", repo, limit)
            
        avg_p_Vals = get_avg(p_Val)
        repo.dropPermanent("avg_p_Vals")
        repo.createPermanent("avg_p_Vals")
        repo['ckarjadi_johnnyg7.avg_p_Vals'].insert_many(avg_p_Vals)

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

       this_script = doc.agent('alg:avg_scripts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
       resource1 = doc.entity('dat:p_Val', {'prov:label':'Property Assessment Report 2016', \
           prov.model.PROV_TYPE:'ont:DataResource'})

       get_avg = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
       doc.wasAssociatedWith(get_avg, this_script)
       doc.usage(get_avg, resource1, startTime, None,
               {prov.model.PROV_TYPE:'ont:Retrieval'
               }
           )

       avg_pvals = doc.entity('dat:ckarjadi_johnnyg7#avg_pvals', {prov.model.PROV_LABEL:'Average Property Values', prov.model.PROV_TYPE:'ont:DataSet'})
       doc.wasAttributedTo(avg_pvals, this_script)
       doc.wasGeneratedBy(avg_pvals, get_avg, endTime)
       doc.wasDerivedFrom(avg_pvals, resource1, get_avg, get_avg, get_avg)
       repo.record(doc.serialize())
       repo.logout()
       return doc

avg_exe.execute()
doc = avg_exe.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))
