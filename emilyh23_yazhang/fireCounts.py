import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'emilyh23_yazhang'
    reads = []
    writes = ['emilyh23_yazhang.fireCounts']    
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emilyh23_yazhang', 'emilyh23_yazhang')
    
        filen = '../data/Fire_311_Service_Requests.json'
        res = open(filen, 'r')
        r5 = json.load(res)
        repo.dropPermanent("Fire_311_Service_Requests")
        repo.createPermanent("Fire_311_Service_Requests")
        repo['emilyh23_yazhang.Fire_311_Service_Requests'].insert_many(r5)
        

        # MAPPING: creates lists of dictionaries that contains fire incidents by category, their districts, and their ontime/delay status, and their lat/long
        fireDep = []
        fire = []
        fireHydrant = []
        fireEstab = []
        
        for dic in r5:
            if (dic['FIELD8'] == 'Fire Department'):
                new_dic = {dic['FIELD8']:dic['FIELD5'], 'District': dic['FIELD17'], 'Latitude': dic['FIELD30'], 'Longitude': dic['FIELD31']}
                fireDep.append(new_dic)
            elif (dic['FIELD8'] == 'Fire in Food Establishment'):
                new_dic = {dic['FIELD8']:dic['FIELD5'], 'District': dic['FIELD17'], 'Latitude': dic['FIELD30'], 'Longitude': dic['FIELD31']}
                fireEstab.append(new_dic)
            elif (dic['FIELD8'] == 'Fire'):
                new_dic = {dic['FIELD8']:dic['FIELD5'], 'District': dic['FIELD17'],'Latitude': dic['FIELD30'], 'Longitude': dic['FIELD31']}
                fire.append(new_dic)
            elif (dic['FIELD8'] == 'Fire Hydrant'):
                new_dic = {dic['FIELD8']:dic['FIELD5'], 'District': dic['FIELD17'],'Latitude': dic['FIELD30'], 'Longitude': dic['FIELD31']}
                fireHydrant.append(new_dic)
        
        # list of districts
        districts = ['1','12','11','3','4','6','7','8','9']
        
        # fireByDis is a list of dictionaries for each district and the frequencies of fire in each districT
        # REDUCE
        fireByDis = [{d: []} for d in districts]
        for dic in fire:
            if (dic['District'] == '1'):
                fireByDis[0]['1'].append(dic) 
            elif (dic['District'] == '12'):
                fireByDis[1]['12'].append(dic) 
            elif (dic['District'] == '11'):
                fireByDis[2]['11'].append(dic) 
            elif (dic['District'] == '3'):
                fireByDis[3]['3'].append(dic)
            elif (dic['District'] == '4'):
                fireByDis[4]['4'].append(dic) 
            elif (dic['District'] == '6'):
                fireByDis[5]['6'].append(dic) 
            elif (dic['District'] == '7'):
                fireByDis[6]['7'].append(dic) 
            elif (dic['District'] == '8'):
                fireByDis[7]['8'].append(dic) 
            elif (dic['District'] == '9'):
                fireByDis[8]['9'].append(dic) 
        
        #s = json.dumps(fireByDis, sort_keys=True, indent=2)
        #print(s)
        disFireCount = [{d: 0} for d in districts]
        for dic in fireByDis:
            for k, v in dic.items():
                if (k=='1'):
                    disFireCount[0]['1'] = {'count':len(dic[k]), 'Type': 'Fire'}
                if (k=='12'):
                    disFireCount[1]['12'] = {'count':len(dic[k]), 'Type': 'Fire'}
                if (k=='11'):
                    disFireCount[2]['11'] = {'count':len(dic[k]), 'Type': 'Fire'}
                if (k=='3'):
                    disFireCount[3]['3'] = {'count':len(dic[k]), 'Type': 'Fire'}
                if (k=='4'):
                    disFireCount[4]['4'] = {'count':len(dic[k]), 'Type': 'Fire'}
                if (k=='6'):
                    disFireCount[5]['6'] = {'count':len(dic[k]), 'Type': 'Fire'}
                if (k=='7'):
                    disFireCount[6]['7'] = {'count':len(dic[k]), 'Type': 'Fire'}
                if (k=='8'):
                    disFireCount[7]['8'] = {'count':len(dic[k]), 'Type': 'Fire'}
                if (k=='9'):
                    disFireCount[8]['9'] = {'count':len(dic[k]), 'Type': 'Fire'}
        
        #s = json.dumps(r, sort_keys=True, indent=2)
        #print(s)

        repo.dropPermanent("fireCounts")
        repo.createPermanent("fireCounts")
        repo['emilyh23_yazhang.fireCounts'].insert_many(disFireCount)
    
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

        this_script = doc.agent('alg:fireCounts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        Fire_311_Service_Requests = doc.entity('dat:Fire_311_Service_Requests', {prov.model.PROV_LABEL:'Fire_311_Service_Requests', prov.model.PROV_TYPE:'ont:DataSet'})
        this_run = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, Fire_311_Service_Requests, startTime)
                
        #New map-reduced dataset
        fireCounts = doc.entity('dat:fireCounts', {prov.model.PROV_LABEL:'fireCounts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fireCounts, this_script)
        doc.wasGeneratedBy(fireCounts, this_run, endTime)
        doc.wasDerivedFrom(fireCounts, Fire_311_Service_Requests, this_run, this_run, this_run)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
