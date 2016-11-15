import dml
import prov.model
import uuid
import datetime
import gpxpy.geo

class liquorAndBPDS(dml.Algorithm):
    contributor = "cyung20_kwleung"
    reads = ['cyung20_kwleung.liquor', 'cyung20_kwleung.BPDS']
    writes = ['cyung20_kwleung.liquorAndBPDS']

    @staticmethod
    def execute(trial = False):
        client = dml.pymongo.MongoClient()
        repo = client.repo 
        repo.authenticate("cyung20_kwleung", "cyung20_kwleung") 
        
        startTime = datetime.datetime.now()
        
        repo.dropPermanent("liquorAndBPDS")
        repo.createPermanent("liquorAndBPDS")
        
        data = repo.cyung20_kwleung.BPDS.find()

        # new is a list storing location name + location coordinates for
        # Boston Police District Stations
        new = []
        names = []
        for d in data:
            a = dict(d)['location']
            lon = (a['coordinates'][0])
            lat = (a['coordinates'][1])
            name = d['name']
            new += [[name, [lon, lat]]]
        
        #print(new)
            
        data = repo.cyung20_kwleung.liquor.find() 
        
        # new is a list storing business name + location coordinates for
        # liquor stores.
        new1 = []
        for d in data:
            a = dict(d)['location']
            businessName = dict(d)['businessname']
            if a['coordinates'][0] != 0 and a['coordinates'][1] != 0:
                new1 += [[businessName, [a['coordinates'][0], a['coordinates'][1]]]]
        
        #print(new1)
        
        # liq contains all liquor stores with NO police stations within a 
        # mile of its location
        liq = {}
        
        # use num to count amount of liquor stores (key = location + str(num))
        num = 1
        
        #new1 refers to liquor stores
        for x in range(0, len(new1)):
            count = 0
            #new refers to BPDS
            #compares distance of each liquor store to all BPDS's.
            for y in range(0, len(new)):
                lon1 = new1[x][1][0]
                lon2 = new[y][1][0]
                lat1 = new1[x][1][1]
                lat2 = new[y][1][1]
                dist = gpxpy.geo.haversine_distance(lat1, lon1, lat2, lon2)
                # 1609 is a mile. 
                # If distance is less than/equal to a mile, increment count
                if dist <= 1609:
                    count += 1
            # Count 0 means a liquor store has no police stations within a mile
            if count == 0:
                liq['location' + str(num)] = new1[x][1], new1[x][0]
                num += 1
        
        for key in liq:
            #add all liquor stores with no police stations within a mile to
            #liquorAndDPDS
            repo.cyung20_kwleung.liquorAndBPDS.insert_one({key: liq[key]})
        
            repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyung20_kwleung', 'cyung20_kwleung')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/cyung20_kwleung') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        
        this_script = doc.agent('alg:liquorAndBPDS', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        BPDS = doc.entity('dat:cyung20_kwleung#BPDS', {prov.model.PROV_LABEL:'Boston Police District Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        liquorLicense = doc.entity('dat:cyung20_kwleung#liquor', {prov.model.PROV_LABEL:'Liquor Licenses', prov.model.PROV_TYPE:'ont:DataSet'})
        
        this_final = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
        
        doc.wasAssociatedWith(this_final, this_script)
        doc.used(this_final, liquorLicense, startTime)
        doc.used(this_final, BPDS, startTime)
        
        liquorAndBPDS = doc.entity('dat:liquorAndBPDS', {prov.model.PROV_LABEL:'Liquor stores and # of police stations nearby', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(liquorAndBPDS, this_script)
        doc.wasGeneratedBy(liquorAndBPDS, this_final, endTime)
        doc.wasDerivedFrom(liquorAndBPDS, BPDS, this_final, this_final, this_final)
        doc.wasDerivedFrom(liquorAndBPDS, liquorLicense, this_final, this_final, this_final)
        
        repo.record(doc.serialize())
        repo.logout()
        
        return doc
        

liquorAndBPDS.execute()
doc = liquorAndBPDS.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

# Print results from new data from liquorAndBPS
'''data = repo.cyung20_kwleung.liquorAndBPDS.find()

for d in data:
    print(dict(d))'''
