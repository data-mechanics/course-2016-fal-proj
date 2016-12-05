import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from correlation_scripts import *

class correlation(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = []
    writes = ['ckarjadi_johnnyg7.f_Est_stats']  

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        avg_p_Vals = get_Col("avg_p_Vals", repo)
        #avg_p_Vals = [ {zip_code: ##, avg: ##}, {zip_code: ##, avg:#}]
        # print(avg_p_Vals)
        f_Pan = get_Col("f_Pan",repo)
        #f_pan = [ {zip_code: ## }]
        z_code = 'zip_code'
        f_Pan_num = count(f_Pan,z_code)
        # count = {01701: 100, 01702: 40} - etc
        f_Pan_stats = match_values(avg_p_Vals,z_code,f_Pan_num)
        get = grab(f_Pan_stats)
        stat_PVAL = p(get[0],get[1])
        f_Pan_stats = [{'f_Pan_correlation_coefficient': stat_PVAL[1],\
                        'f_Pan_pVAL': stat_PVAL[0]}]
        
        c_Gard = get_Col('c_Gard',repo)
        change = change_gardens(c_Gard)
        #c_Gard has zip_code '2215'have to change to '02215'
        c_Gard_num = count(change,z_code)
        c_Gard_stats = match_values(avg_p_Vals,z_code,c_Gard_num)
        get = grab(c_Gard_stats)
        stat_PVAL = p(get[0],get[1])
        c_Gard_stats = [{'c_Gard_correlation_coefficient': stat_PVAL[1],\
                         'c_Gard_pVAL': stat_PVAL[0]}]
        
        f_Estab = get_Col('f_Estab',repo)
        change = change_food(f_Estab)
        f_Estab_num = count(change,z_code)
        f_Estab_stats = match_values(avg_p_Vals,z_code,f_Estab_num)
        get = grab(f_Estab_stats)
        stat_PVAL = p(get[0],get[1])
        f_Estab_stats = [{'f_Estab_correlation_coefficient': stat_PVAL[1],\
                          'f_Estab_pVAL': stat_PVAL[0]}]
        
        c_Inc = get_Col('c_Inc',repo)
        change = change_crime(c_Inc)
        c_Inc_num = count(change,z_code)
        c_Inc_stats = match_values(avg_p_Vals,z_code,c_Inc_num)
        get = grab(c_Inc_stats)
        stat_PVAL = p(get[0],get[1])
        c_Inc_stats = [{'c_Inc_correlation_coefficient': stat_PVAL[1],\
                        'c_Inc_PVAL': stat_PVAL[0]}]
        #print(c_Inc_stats)
        #print(change)
        
        repo.dropPermanent("f_Pan_stats")
        repo.createPermanent("f_Pan_stats")
        repo['ckarjadi_johnnyg7.f_Pan_stats'].insert_many(f_Pan_stats)
        
        repo.dropPermanent("c_Gard_stats")
        repo.createPermanent("c_Gard_stats")
        repo['ckarjadi_johnnyg7.c_Gard_stats'].insert_many(c_Gard_stats)

        repo.dropPermanent("f_Estab_stats")
        repo.createPermanent("f_Estab_stats")
        repo['ckarjadi_johnnyg7.f_Estab_stats'].insert_many(f_Estab_stats)


        repo.dropPermanent("c_Inc_stats")
        repo.createPermanent("c_Inc_stats")
        repo['ckarjadi_johnnyg7.c_Inc_stats'].insert_many(c_Inc_stats)        
        
 
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

        this_script = doc.agent('alg:correlation_scripts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:corr_coe', {'prov:label':'Correlation Coefficient', \
            prov.model.PROV_TYPE:'ont:DataResource'})
        get_corr_coe = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_corr_coe, this_script)
        doc.usage(get_corr_coe, resource1, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                }
            )

        corr_coe = doc.entity('dat:ckarjadi_johnnyg7#corr_coe', {prov.model.PROV_LABEL:'Correlation Coefficient', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(corr_coe, this_script)
        doc.wasGeneratedBy(corr_coe, get_corr_coe, endTime)
        doc.wasDerivedFrom(corr_coe, resource1, get_corr_coe, get_corr_coe, get_corr_coe)
        repo.record(doc.serialize())
        repo.logout()
        return doc

correlation.execute()
doc = correlation.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))


