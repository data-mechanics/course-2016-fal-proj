import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from correlation_scripts import *

class correlation(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = ['ckarjadi_johnnyg7.avg_p_Vals','ckarjadi_johnnyg7.h_Locs']
    reads+= ['ckarjadi_johnnyg7.f_Pan','ckarjadi_johnnyg7.c_Gard']
    reads+= ['ckarjadi_johnnyg7.f_Estab', 'ckarjadi_johnnyg7.c_Inc']
    
    writes = ['ckarjadi_johnnyg7.f_Estab_stats','ckarjadi_johnnyg7.h_Locs_stats']
    writes+= ['ckarjadi_johnnyg7.f_Pan_stats','ckarjadi_johnnyg7.c_Gard_stats']
    writes+= ['ckarjadi_johnnyg7.c_Inc_stats']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''

        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        limit = 250 #if trial == True, grab only 250 entries from DB
        #all the reads
        if trial == False:
            avg_p_Vals = get_Col("avg_p_Vals",repo)
            h_Loc = get_Col("h_Locs",repo)
            f_Pan = get_Col("f_Pan",repo)
            c_Gard = get_Col('c_Gard',repo)   

            f_Estab = get_Col('f_Estab',repo)
            c_Inc = get_Col('c_Inc',repo)
        else:
            avg_p_Vals = get_Short("avg_p_Vals",repo,limit)
            h_Loc = get_Short("h_Locs",repo,limit)
            f_Pan = get_Short("f_Pan",repo,limit)
            c_Gard = get_Short('c_Gard',repo,limit)   

            f_Estab = get_Short('f_Estab',repo,limit)
            c_Inc = get_Short('c_Inc',repo,limit)

            
        #begin all the writes
        
        #h_loc = [ {zipcode: ##, location: {coordinates:[long,lat]}}]
        #begin creation of h_Loc_X_avg_p_Vals
        h_Loc = change_hospitals(h_Loc)
        z_code = 'zipcode'
        h_Loc_num = count(h_Loc,z_code)
        h_Loc_X_avg_p_Vals = create_intersect(avg_p_Vals,h_Loc_num,'zip_code')
        
        #h_loc_num = { 02111: #_hospitals in that 02111, 02112: ##, ...}
        #begin creation of h_Loc_stats
        h_Loc_stats = match_values(avg_p_Vals,z_code,h_Loc_num)
        get = grab(h_Loc_stats)
        stat_HLOC = p(get[0],get[1])
        h_Loc_stats = [{'h_loc_correlation_coefficient': stat_HLOC[1],\
                        'h_loc_pVAL': stat_HLOC[0]}]
        
    
        
        #begin creation of f_Pan_Stats
        #f_pan = [ {zip_code: ## }]
        z_code = 'zip_code'
        f_Pan_num = count(f_Pan,z_code)
        # count = {01701: 100, 01702: 40} - etc
        f_Pan_stats = match_values(avg_p_Vals,z_code,f_Pan_num)
        get = grab(f_Pan_stats)
        stat_FPAN = p(get[0],get[1])
        f_Pan_stats = [{'f_Pan_correlation_coefficient': stat_FPAN[1],\
                        'f_Pan_pVAL': stat_FPAN[0]}]
        

        #begin creation of c_Gard_stats
        change = change_gardens(c_Gard)
        #c_Gard has zip_code '2215'have to change to '02215'
        c_Gard_num = count(change,z_code)
        c_Gard_stats = match_values(avg_p_Vals,z_code,c_Gard_num)
        get = grab(c_Gard_stats)
        stat_CGARD = p(get[0],get[1])
        c_Gard_stats = [{'c_Gard_correlation_coefficient': stat_CGARD[1],\
                         'c_Gard_pVAL': stat_CGARD[0]}]
        
        #begin creation of f_Estab_stats
        change = change_food(f_Estab)
        f_Estab_num = count(change,z_code)
        f_Estab_stats = match_values(avg_p_Vals,z_code,f_Estab_num)
        get = grab(f_Estab_stats)
        stat_FESTAB = p(get[0],get[1])
        f_Estab_stats = [{'f_Estab_correlation_coefficient': stat_FESTAB[1],\
                          'f_Estab_pVAL': stat_FESTAB[0]}]
        
        #begin creation of c_Inc_stats
        change = change_crime(c_Inc)
        c_Inc_num = count(change,z_code)
        c_Inc_stats = match_values(avg_p_Vals,z_code,c_Inc_num)
        get = grab(c_Inc_stats)
        stat_CINC = p(get[0],get[1])
        c_Inc_stats = [{'c_Inc_correlation_coefficient': stat_CINC[1],\
                        'c_Inc_PVAL': stat_CINC[0]}]

        
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
        

        repo.dropPermanent("h_Loc_X_avg_p_Vals")
        repo.createPermanent("h_Loc_X_avg_p_Vals")
        repo['ckarjadi_johnnyg7.h_Loc_X_avg_p_Vals'].insert_many(h_Loc_X_avg_p_Vals)
        
        repo.dropPermanent("h_Loc_stats")
        repo.createPermanent("h_Loc_stats")
        repo['ckarjadi_johnnyg7.h_Loc_stats'].insert_many(h_Loc_stats)
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

        f_Pan_R = doc.entity('dat:f_Pan', {'prov:label':'Food Pantries', \
            prov.model.PROV_TYPE:'ont:DataResource'})
        c_Gard_R = doc.entity('dat:c_Gard', {'prov:label':'Community Gardens', \
            prov.model.PROV_TYPE:'ont:DataResource'})
        f_Estab_R = doc.entity('dat:f_Estab', {'prov:label':'Food Establishments', \
            prov.model.PROV_TYPE:'ont:DataResource'})
        c_Inc_R = doc.entity('dat:c_Inc', {'prov:label':'Crime Incident Reports', \
            prov.model.PROV_TYPE:'ont:DataResource'})

        h_Locs_R = doc.entity('dat:h_Locs', {'prov:label':'Hospital Locations', \
            prov.model.PROV_TYPE:'ont:DataResource'})
        
        f_Pan_TT = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        c_Gard_TT= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        f_Estab_TT= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        c_Inc_TT= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        h_Locs_TT= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(f_Pan_TT, this_script)
        doc.wasAssociatedWith(c_Gard_TT, this_script)
        doc.wasAssociatedWith(f_Estab_TT, this_script)
        doc.wasAssociatedWith(c_Inc_TT, this_script)
        doc.wasAssociatedWith(h_Locs_TT, this_script)
        
        doc.usage(f_Pan_TT, f_Pan_R, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                }
            )
        doc.usage(c_Gard_TT, c_Gard_R, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                }
            )
        doc.usage(f_Estab_TT, f_Estab_R, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                }
            )
        doc.usage(c_Inc_TT, c_Inc_R, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                }
            )
        doc.usage(h_Locs_TT, h_Locs_R, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                }
            )
    

        f_Pan_CC = doc.entity('dat:ckarjadi_johnnyg7#f_Pan_stats', {prov.model.PROV_LABEL:'Food Pantries Statistics', prov.model.PROV_TYPE:'ont:DataSet'})
        c_Gard_CC = doc.entity('dat:ckarjadi_johnnyg7#c_Gard_stats', {prov.model.PROV_LABEL:'Community Gardens Statistics', prov.model.PROV_TYPE:'ont:DataSet'})
        f_Estab_CC= doc.entity('dat:ckarjadi_johnnyg7#f_Estab_stats', {prov.model.PROV_LABEL:'Food Establishments Statistics', prov.model.PROV_TYPE:'ont:DataSet'})
        c_Inc_CC= doc.entity('dat:ckarjadi_johnnyg7#c_Inc_stats', {prov.model.PROV_LABEL:'Crime Incidents Statistics', prov.model.PROV_TYPE:'ont:DataSet'})
        h_Locs_CC= doc.entity('dat:ckarjadi_johnnyg7#h_Loc_stats', {prov.model.PROV_LABEL:'Food Pantries Statistics', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(f_Pan_CC, this_script)
        doc.wasAttributedTo(c_Gard_CC, this_script)
        doc.wasAttributedTo(f_Estab_CC, this_script)
        doc.wasAttributedTo(c_Inc_CC, this_script)
        doc.wasAttributedTo(h_Locs_CC, this_script)

        
        doc.wasGeneratedBy(f_Pan_CC, f_Pan_TT, endTime)
        doc.wasGeneratedBy(c_Gard_CC, c_Gard_TT, endTime)
        doc.wasGeneratedBy(f_Estab_CC, f_Estab_TT, endTime)
        doc.wasGeneratedBy(c_Inc_CC, c_Inc_TT, endTime)
        doc.wasGeneratedBy(h_Locs_CC, h_Locs_TT, endTime)
    

        
        doc.wasDerivedFrom(f_Pan_CC, f_Pan_R, f_Pan_TT, f_Pan_TT, f_Pan_TT)
        doc.wasDerivedFrom(c_Gard_CC, c_Gard_R, c_Gard_TT, c_Gard_TT, c_Gard_TT)
        doc.wasDerivedFrom(f_Estab_CC, f_Estab_R, f_Estab_TT, f_Estab_TT, f_Estab_TT)
        doc.wasDerivedFrom(c_Inc_CC, c_Inc_R, c_Inc_TT, c_Inc_TT, c_Inc_TT)
        doc.wasDerivedFrom(h_Locs_CC, h_Locs_R, h_Locs_TT, h_Locs_TT, h_Locs_TT)


        repo.record(doc.serialize())
        repo.logout()
        return doc

correlation.execute()
doc = correlation.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))
