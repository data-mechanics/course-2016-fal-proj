import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'emilyh23_yazhang'
    reads = []
    writes = ['emilyh23_yazhang.hydrantCounts']    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emilyh23_yazhang', 'emilyh23_yazhang')
        
        r5 = repo.emilyh23_yazhang.Fire_311_Service_Requests.find() 

        # MAPPING: creates lists of dictionaries that contains fire incidents by category, their districts, and their ontime/delay status, and their lat/long
        fireDep = []
        fire = []
        fireHydrant = []
        fireEstab = []
        
        for dic in r5:
            if (dic['CASE_TITLE'] == 'Fire Department'):
                new_dic = {dic['CASE_TITLE']:dic['OnTime_Status'], 'District': dic['fire_district'], 'Latitude': dic['LATITUDE'], 'Longitude': dic['LONGITUDE']}
                fireDep.append(new_dic)
            elif (dic['CASE_TITLE'] == 'Fire in Food Establishment'):
                new_dic = {dic['CASE_TITLE']:dic['OnTime_Status'], 'District': dic['fire_district'], 'Latitude': dic['LATITUDE'], 'Longitude': dic['LONGITUDE']}
                fireEstab.append(new_dic)
            elif (dic['CASE_TITLE'] == 'Fire'):
                new_dic = {dic['CASE_TITLE']:dic['OnTime_Status'], 'District': dic['fire_district'],'Latitude': dic['LATITUDE'], 'Longitude': dic['LONGITUDE']}
                fire.append(new_dic)
            elif (dic['CASE_TITLE'] == 'Fire Hydrant'):
                new_dic = {dic['CASE_TITLE']:dic['OnTime_Status'], 'District': dic['fire_district'],'Latitude': dic['LATITUDE'], 'Longitude': dic['LONGITUDE']}
                fireHydrant.append(new_dic)
        
        # list of districts
        districts = ['1','12','11','3','4','6','7','8','9']
        
        # fireHydrantByDis is a list of dictionaries for fire hydrant repairs sorted by district
        # REDUCE
        fireHydrantByDis = [{d: []} for d in districts]
        for dic in fireHydrant:
            if (dic['District'] == '1'):
                fireHydrantByDis[0]['1'].append(dic) 
            elif (dic['District'] == '12'):
                fireHydrantByDis[1]['12'].append(dic) 
            elif (dic['District'] == '11'):
                fireHydrantByDis[2]['11'].append(dic) 
            elif (dic['District'] == '3'):
                fireHydrantByDis[3]['3'].append(dic)
            elif (dic['District'] == '4'):
                fireHydrantByDis[4]['4'].append(dic) 
            elif (dic['District'] == '6'):
                fireHydrantByDis[5]['6'].append(dic) 
            elif (dic['District'] == '7'):
                fireHydrantByDis[6]['7'].append(dic) 
            elif (dic['District'] == '8'):
                fireHydrantByDis[7]['8'].append(dic) 
            elif (dic['District'] == '9'):
                fireHydrantByDis[8]['9'].append(dic) 

        # number of fire hydrant repairs by district
        fireHydrantCount = [{d: 0} for d in districts]
        for dic in fireHydrantByDis:
            for k, v in dic.items():
                if (k=='1'):
                    fireHydrantCount[0]['1'] = {'count':len(dic[k]), 'Type': 'Fire Hydrant'}
                if (k=='12'):
                    fireHydrantCount[1]['12'] = {'count':len(dic[k]), 'Type': 'Fire Hydrant'}
                if (k=='11'):
                    fireHydrantCount[2]['11'] = {'count':len(dic[k]), 'Type': 'Fire Hydrant'}
                if (k=='3'):
                    fireHydrantCount[3]['3'] = {'count':len(dic[k]), 'Type': 'Fire Hydrant'}
                if (k=='4'):
                    fireHydrantCount[4]['4'] = {'count':len(dic[k]), 'Type': 'Fire Hydrant'}
                if (k=='6'):
                    fireHydrantCount[5]['6'] = {'count':len(dic[k]), 'Type': 'Fire Hydrant'}
                if (k=='7'):
                    fireHydrantCount[6]['7'] = {'count':len(dic[k]), 'Type': 'Fire Hydrant'}
                if (k=='8'):
                    fireHydrantCount[7]['8'] = {'count':len(dic[k]), 'Type': 'Fire Hydrant'}
                if (k=='9'):
                    fireHydrantCount[8]['9'] = {'count':len(dic[k]), 'Type': 'Fire Hydrant'}

        repo.dropPermanent("hydrantCounts")
        repo.createPermanent("hydrantCounts")
        repo['emilyh23_yazhang.hydrantCounts'].insert_many(fireHydrantCount)
    
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/api/views/')

        this_script = doc.agent('alg:emilyh23_yazhang#hydrantCounts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_hydrantCounts = doc.entity('dat:emilyh23_yazhang#Fire_311_Service_Requests', {'prov:label':'Fire_311_Service_Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource_hydrantCounts, startTime)
                
        #New map-reduced dataset
        hydrantCounts = doc.entity('dat:emilyh23_yazhang#hydrantCounts', {prov.model.PROV_LABEL:'hydrantCounts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hydrantCounts, this_script)
        doc.wasGeneratedBy(hydrantCounts, this_run, endTime)
        doc.wasDerivedFrom(hydrantCounts, resource_hydrantCounts, this_run, this_run, this_run)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
