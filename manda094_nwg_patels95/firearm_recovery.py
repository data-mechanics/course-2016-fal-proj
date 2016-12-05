import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class firearm_recovery(dml.Algorithm):
    contributor = 'manda094_nwg_patels95'
    reads = ['manda094_nwg_patels95.crimes']
    writes = ['manda094_nwg_patels95.firearm_recovery']

    # I use set(R) so the keys are a list of unique dates
    def aggregate(R, f):
        keys = {r[0] for r in set(R)}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

        with open('../auth.json') as jsonFile:
            auth = json.load(jsonFile)

        socrataAppToken = auth["services"]["cityofbostondataportal"]["token"]

        # Boston Police Department Firearms Recovery Counts
        url = 'https://data.cityofboston.gov/resource/ffz3-2uqv.json?$$app_token=' + socrataAppToken
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        crimeData = []
        for crime in repo['manda094_nwg_patels95.crimes'].find():
            crimeData.append(crime)

        # crimeTuples will be used in the aggregate function
        crimeTuples = [(c['fromdate'], 1) for c in crimeData]
        totalCrimesPerDate = firearm_recovery.aggregate(crimeTuples, sum)

        for i in range(len(r)):
            total = int(r[i]['crimegunsrecovered']) + int(r[i]['gunssurrenderedsafeguarded']) + \
             int(r[i]['buybackgunsrecovered'])
            r[i]['totalgunsrecovered'] = total
            r[i]['collectiondate'] = r[i]['collectiondate'][:10]
            for t in totalCrimesPerDate:
                if t[0] == r[i]['collectiondate']:
                    r[i]['totalcrimes'] = t[1]
            arr = []
            # compnos is a unique id for each crime
            for c in crimeData:
                if r[i]['collectiondate'] == c['fromdate']:
                    if 'compnos' in c:
                        arr.append(c['compnos'])
            r[i]['crimecompnoslist'] = arr

        repo.dropPermanent("firearm_recovery")
        repo.createPermanent("firearm_recovery")
        repo['manda094_nwg_patels95.firearm_recovery'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
       client = dml.pymongo.MongoClient()
       repo = client.repo
       repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

       doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
       doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
       doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
       doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
       doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

       this_script = doc.agent('alg:manda094_nwg_patels95#firearm_recovery', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
       resource1 = doc.entity('bdp:ffz3-2uqv', {'prov:label':'BPD Firearm Recovery', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
       resource2 = doc.entity('dat:manda094_nwg_patels95#crimes', {'prov:label':'Crimes', prov.model.PROV_TYPE:'ont:DataResource'})
       this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
       doc.wasAssociatedWith(this_run, this_script)
       doc.usage(this_run, resource1, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
       doc.usage(this_run, resource2, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

       firearm_recovery_counts = doc.entity('dat:manda094_nwg_patels95#firearm_recovery', {prov.model.PROV_LABEL:'Firearm Recovery', prov.model.PROV_TYPE:'ont:DataSet'})
       doc.wasAttributedTo(firearm_recovery_counts, this_script)
       doc.wasGeneratedBy(firearm_recovery_counts, this_run, endTime)
       doc.wasDerivedFrom(firearm_recovery_counts, resource1, this_run, this_run, this_run)
       doc.wasDerivedFrom(firearm_recovery_counts, resource2, this_run, this_run, this_run)

       repo.record(doc.serialize())
       repo.logout()

       return doc

firearm_recovery.execute()
doc = firearm_recovery.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
