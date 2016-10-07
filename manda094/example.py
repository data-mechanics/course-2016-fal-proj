import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'manda094'
    reads = []
    writes = ['manda094.foodbanks', 'manda094.childrenfeed']

    @staticmethod 
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094','manda094')

#DATA TRANSFORMATION 1 START
        #food pantry dataset 
        url = "https://data.cityofboston.gov/resource/4tie-bhxw.json?area=Boston&$limit=3"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("foodbanks")
        repo.createPermanent("foodbanks")
        repo['manda094.foodbanks'].insert_many(r)

       #children feeding program 
        url = "https://data.cityofboston.gov/resource/6s7x-jq48.json?location_1_city=BOSTON&$limit=5"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("childrenfeed")
        repo.createPermanent("chprogram")
        repo['manda094.chprogram'].insert_many(r)

        repo.dropPermanent("tFoodHelp")  #combined dataset 1
        repo.createPermanent("tFoodHelp")

        for data in repo['manda094.foodbanks'].find():
              repo['manda094.tFoodHelp'].insert(data)
        for dataCH in repo ['manda094.chprogram'].find():
              repo['manda094.tFoodHelp'].insert(dataCH)        

#DATA TRANSFORMATION 2 START 

        #311 service calls dataset
        url = "https://data.cityofboston.gov/resource/rtbk-4hc4.json?case_title=Unsatisfactory%20Living%20Conditions&$limit=5"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("servicecalls")
        repo.createPermanent("servicecalls")
        repo['manda094.servicecalls'].insert_many(r)

        #Code Enforcement dataset
        url = "https://data.cityofboston.gov/resource/w39n-pvs8.json?description=Unsafe%20Structure&$limit=5"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("codeEnforce")
        repo.createPermanent("codeEnforce")
        repo['manda094.codeEnforce'].insert_many(r)

        repo.dropPermanent("totalBuild")
        repo.createPermanent("totalBuild")

        for data in repo['manda094.servicecalls'].find():
            repo['manda094.totalBuild'].insert(data)
        for dataCode in repo ['manda094.codeEnforce'].find():
            repo['manda094.totalBuild'].insert(dataCode) 

        
#DATA TRANSFORMATION 2 END

#DATA TRANSFORMATION 3 START

        #Crime Incident Reports - East Boston 
        url = "https://data.cityofboston.gov/resource/ufcx-3fdn.json?reptdistrict=A7&$limit=5"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("crimeIn")
        repo.createPermanent("crimeIn")
        repo['manda094.crimeIn'].insert_many(r)

        repo.dropPermanent("totalCrime&Build")
        repo.createPermanent("totalCrime&Build")



        for data in repo['manda094.crimeIn'].find():
            repo['manda094.totalCrime&Build'].insert(data)
        for data in repo ['manda094.totalBuild'].find():
            repo['manda094.totalCrime&Build'].insert(data) 

#DATA TRANSFORMATION 3 END 
       
        repo.logout()

        endTime = datetime.datetime.now()

    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094','manda094')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        #MAIN SCRIPT
        this_script = doc.agent('alg:manda094#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
       
#all provance for foodbank and children feeding program transition START
        chprogram_resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Children Feed Program', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        foodbank_resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Food Banks', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_foodbank = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_chprogram = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        
        doc.wasAssociatedWith(get_foodbank, this_script)
        doc.wasAssociatedWith(get_chprogram, this_script)
        
        doc.usage(get_foodbank, foodbank_resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?area=Boston&$select=area,zip_code,OPEN_DT'})
        doc.usage(get_chprogram, chprogram_resource, startTime, None,{prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?location_1_city=BOSTON&$select=location_1_city,businessname,location_1_zip,OPEN_DT'})

        #created database from food bank and children feeding program datasets 
        totalFoodData = doc.entity('dat:tFoodHelp', {prov.model.PROV_LABEL:'Total Food Help in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(totalFoodData, this_script)
        doc.wasGeneratedBy(totalFoodData, get_chprogram, endTime)
        doc.wasDerivedFrom(totalFoodData, chprogram_resource, foodbank_resource)
#all provance for foodbank and children feeding program transition END

#311 Service/Code Enforcement Combo Start  
        service_resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        codeEn_resource= doc.entity('bdp:wc8w-nujj', {'prov:label':'Code Enforcement', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_service = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_codeEn = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        
        doc.wasAssociatedWith(get_service, this_script)
        doc.wasAssociatedWith(get_codeEn, this_script)
        
        doc.usage(get_service, service_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval', 'ont:Query':'?type=Unsatisfactory+Living+Conditions&$select=type,neighborhood,OPEN_DT'    }  )
        doc.usage(get_codeEn, codeEn_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval',
                                                                       'ont:Query':'city=Boston&$select=city,description,OPEN_DT'})

        totalBuildingData = doc.entity('dat:totalBuild', {prov.model.PROV_LABEL:'Total Unsafe Buildings in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(totalBuildingData, this_script)
        doc.wasGeneratedBy(totalBuildingData, get_codeEn, endTime)
        doc.wasDerivedFrom(totalBuildingData, codeEn_resource, service_resource)
        

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc
    
example.execute()
doc = example.provenance()

#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
