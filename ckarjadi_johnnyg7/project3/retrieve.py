import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from write_file import *
class retrieve(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = []
    writes = ['ckarjadi_johnnyg7.p_Val', 'ckarjadi_johnnyg7.f_Pan','ckarjadi_johnnyg7.c_Inc']
    writes +=['ckarjadi_johnnyg7.w_Jams', 'ckarjadi_johnnyg7.f_Estab','ckarjadi_johnnyg7.c_Gard.']
    writes +=['ckarjadi_johnnyg7.h_Locs']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')

        if trial == False:
            url ='https://data.cityofboston.gov/resource/g5b5-xrwi.json?$limit=170000&$select=zipcode,av_total'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
    
            repo.dropPermanent("p_Val")
            repo.createPermanent("p_Val")
            repo['ckarjadi_johnnyg7.p_Val'].insert_many(r)

            url='https://data.cityofboston.gov/resource/4tie-bhxw.json?$select=zip_code'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
     
            repo.dropPermanent("f_Pan")
            repo.createPermanent("f_Pan")
            repo['ckarjadi_johnnyg7.f_Pan'].insert_many(r)
            
            url='https://data.cityofboston.gov/resource/rdqf-ter7.json?$select=zip_code'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            
            repo.dropPermanent("c_Gard")
            repo.createPermanent("c_Gard")
            repo['ckarjadi_johnnyg7.c_Gard'].insert_many(r)
            
            url='https://data.cityofboston.gov/resource/fdxy-gydq.json?$select=businessname,zip&$limit=3000'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
     
            repo.dropPermanent("f_Estab")
            repo.createPermanent("f_Estab")
            repo['ckarjadi_johnnyg7.f_Estab'].insert_many(r)
            
            url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json?$select=district,lat,long&$limit=140000'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            
            repo.dropPermanent("c_Inc")
            repo.createPermanent("c_Inc")
            repo['ckarjadi_johnnyg7.c_Inc'].insert_many(r)

            url = 'https://data.cityofboston.gov/resource/pvhv-55ac.json?$select=locx,locy&$limit=100000'
            
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            
            repo.dropPermanent("w_Jams")
            repo.createPermanent("w_Jams")
            repo['ckarjadi_johnnyg7.w_Jams'].insert_many(r)
           
            url = 'https://data.cityofboston.gov/resource/u6fv-m8v4.json?$select=location,zipcode,name'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            
            repo.dropPermanent("h_Locs")
            repo.createPermanent("h_Locs")
            repo['ckarjadi_johnnyg7.h_Locs'].insert_many(r)

        else:
            url ='https://data.cityofboston.gov/resource/g5b5-xrwi.json?$limit=100&$select=zipcode,av_total'

            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)

            repo.dropPermanent("p_Val")
            repo.createPermanent("p_Val")
            repo['ckarjadi_johnnyg7.p_Val'].insert_many(r)
            #filename = 'p_Val'
            #write_file(filename,r)

            url='https://data.cityofboston.gov/resource/4tie-bhxw.json?$select=zip_code&$limit=100'

            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
     
           
            repo.dropPermanent("f_Pan")
            repo.createPermanent("f_Pan")
            repo['ckarjadi_johnnyg7.f_Pan'].insert_many(r)
            print(r)
            
            url='https://data.cityofboston.gov/resource/rdqf-ter7.json?$select=zip_code&$limit=100'
            
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            
            repo.dropPermanent("c_Gard")
            repo.createPermanent("c_Gard")
            repo['ckarjadi_johnnyg7.c_Gard'].insert_many(r)
            
            url='https://data.cityofboston.gov/resource/fdxy-gydq.json?$select=businessname,zip&$limit=100'
            
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            
            repo.dropPermanent("f_Estab")
            repo.createPermanent("f_Estab")
            repo['ckarjadi_johnnyg7.f_Estab'].insert_many(r)
            
            
            url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json?$select=district,lat,long&$limit=100'
            
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            
            repo.dropPermanent("c_Inc")
            repo.createPermanent("c_Inc")
            repo['ckarjadi_johnnyg7.c_Inc'].insert_many(r)
            
            url = 'https://data.cityofboston.gov/resource/pvhv-55ac.json?$select=locx,locy&$limit=100'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            
            repo.dropPermanent("w_Jams")
            repo.createPermanent("w_Jams")
            repo['ckarjadi_johnnyg7.w_Jams'].insert_many(r)
           
            url = 'https://data.cityofboston.gov/resource/u6fv-m8v4.json?$select=location,zipcode,name&$limit=100'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            
            repo.dropPermanent("h_Locs")
            repo.createPermanent("h_Locs")
            repo['ckarjadi_johnnyg7.h_Locs'].insert_many(r)
        
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
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        #<ckarjadi_johnnyg7>#<somefile_name>
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ckarjadi_johnnyg7#retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_p_Val = doc.entity('bdp:g5b5-xrwi', {'prov:label':'Property Values 2016', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_f_Pan = doc.entity('bdp:4tie-bhxw', {'prov:label':'Food Pantries', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        resource_f_Estab = doc.entity('bdp:fdxy-gydq', {'prov:label':'Food Establishments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_c_Gard = doc.entity('bdp:rdqf-ter7', {'prov:label':'Community Gardens', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension:':'json'})

        resource_h_Loc = doc.entity('bdp:u6fv-m8v4', {'prov:label':'Hospitals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension:':'json'})
        resource_c_Inc = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime Occurences', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension:':'json'})
        resource_w_Jams = doc.entity('bdp:29yf-ye7n', {'prov:label':'Traffic Jams', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension:':'json'})

        get_p_Val = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_f_Pan = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        get_f_Estab = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_c_Gard = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        get_h_Loc = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_c_Inc = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_w_Jams = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_p_Val, this_script)
        doc.wasAssociatedWith(get_f_Pan, this_script)

        doc.wasAssociatedWith(get_f_Estab, this_script)
        doc.wasAssociatedWith(get_c_Gard, this_script) 

        doc.wasAssociatedWith(get_h_Loc, this_script)
        doc.wasAssociatedWith(get_c_Inc, this_script)
        doc.wasAssociatedWith(get_w_Jams, this_script)

        p_Val = doc.entity('dat:ckarjadi_johnnyg7#p_Val', {prov.model.PROV_LABEL:'Property Value 2016', prov.model.PROV_TYPE:'ont:DataSet'})
        f_Pan = doc.entity('dat:ckarjadi_johnnyg7#f_Pan', {prov.model.PROV_LABEL:'Food Pantries', prov.model.PROV_TYPE:'ont:DataSet'})
        f_Estab = doc.entity('dat:ckarjadi_johnnyg7#f_Estab', {prov.model.PROV_LABEL:'Food Establishments', prov.model.PROV_TYPE:'ont:DataSet'})
        c_Gard = doc.entity('dat:ckarjadi_johnnyg7#c_Gard', {prov.model.PROV_LABEL:'Community Gardens', prov.model.PROV_TYPE:'ont:DataSet'})
        h_Loc = doc.entity('dat:ckarjadi_johnnyg7#h_Loc', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        c_Inc = doc.entity('dat:ckarjadi_johnnyg7#c_Inc', {prov.model.PROV_LABEL:'Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        w_Jams = doc.entity('dat:ckarjadi_johnnyg7#w_Jams', {prov.model.PROV_LABEL:'Waze Jams', prov.model.PROV_TYPE:'ont:DataSet'})


        doc.usage(p_Val, resource_p_Val, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'$limit=170000&$select=zipcode,av_total'
                }
            )
        doc.usage(f_Pan, resource_f_Pan, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'$select=zip_code'
                }
            )
        doc.usage(f_Estab, resource_f_Estab, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:QUERY':'$select=businessname,zip&$limit=3000'
                }
            )
        doc.usage(c_Gard, resource_c_Gard, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:QUERY':'$select=zip_code'
                }
            )
        doc.usage(h_Loc, resource_h_Loc, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:QUERY':'$select=location,zipcode,name'
                }
            )
        doc.usage(c_Inc, resource_c_Inc, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:QUERY':'$select=district,lat,long&$limit=140000'
                }
            )
        doc.usage(w_Jams, resource_w_Jams, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:QUERY':'$select=locx,locy&$limit=100000'
                }
            )

        doc.wasAttributedTo(p_Val, this_script)
        doc.wasGeneratedBy(p_Val, get_p_Val, endTime)
        doc.wasDerivedFrom(p_Val, resource_p_Val, get_p_Val, get_p_Val, get_p_Val)
        
        doc.wasAttributedTo(f_Pan, this_script)
        doc.wasGeneratedBy(f_Pan, get_f_Pan, endTime)
        doc.wasDerivedFrom(f_Pan, resource_f_Pan, get_f_Pan, get_f_Pan, get_f_Pan)
    
        doc.wasAttributedTo(f_Estab, this_script)
        doc.wasGeneratedBy(f_Estab, get_f_Estab, endTime)
        doc.wasDerivedFrom(f_Estab, resource_f_Estab, get_f_Estab, get_f_Estab, get_f_Estab)

        doc.wasAttributedTo(c_Gard, this_script)
        doc.wasGeneratedBy(c_Gard, get_c_Gard, endTime)
        doc.wasDerivedFrom(c_Gard, resource_c_Gard, get_c_Gard, get_c_Gard, get_c_Gard) 

        doc.wasAttributedTo(h_Loc, this_script)
        doc.wasGeneratedBy(h_Loc, get_h_Loc, endTime)
        doc.wasDerivedFrom(h_Loc, resource_h_Loc, get_h_Loc, get_h_Loc, get_h_Loc) 

        doc.wasAttributedTo(c_Inc, this_script)
        doc.wasGeneratedBy(c_Inc, get_c_Inc, endTime)
        doc.wasDerivedFrom(c_Inc, resource_c_Inc, get_c_Inc, get_c_Inc, get_c_Inc) 

        doc.wasAttributedTo(w_Jams, this_script)
        doc.wasGeneratedBy(w_Jams, get_w_Jams, endTime)
        doc.wasDerivedFrom(w_Jams, resource_w_Jams, get_w_Jams, get_w_Jams, get_w_Jams) 

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

retrieve.execute()
doc = retrieve.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))
