import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crimes(dml.Algorithm):
    contributor = 'manda094_nwg_patels95'
    reads = []
    writes = ['manda094_nwg_patels95.crimes']

    def select(R, s):
        return [t for t in R if s(t)]

    # return true if the weapontype is a firearm
    def firearm_only(crime):
        if crime['weapontype'] == 'Firearm':
            return True
        else:
            return False

    # return true if the crime date is between 8/20/14 and 7/27/15
    def date_check(crime):
        startDate = datetime.datetime.strptime('2014-08-20', '%Y-%m-%d')
        endDate = datetime.datetime.strptime('2015-07-27', '%Y-%m-%d')
        crimeDate = datetime.datetime.strptime(crime['fromdate'][:10], '%Y-%m-%d')

        if crimeDate >= startDate and crimeDate <= endDate:
            crime['fromdate'] = crime['fromdate'][:10]
            return True
        else:
            return False


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

        # Crime Incident Reports (July 2012 - August 2015)
        url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json?$limit=300000&$$app_token=' + socrataAppToken
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
 

        data = crimes.select(r, crimes.firearm_only)
        data = crimes.select(data, crimes.date_check)

        repo.dropPermanent("crimes")
        repo.createPermanent("crimes")
        repo['manda094_nwg_patels95.crimes'].insert_many(data)

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

        this_script = doc.agent('alg:manda094_nwg_patels95#crimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        crime_reports = doc.entity('dat:manda094_nwg_patels95#crimes', {prov.model.PROV_LABEL:'Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_reports, this_script)
        doc.wasGeneratedBy(crime_reports, this_run, endTime)
        doc.wasDerivedFrom(crime_reports, resource, this_run, this_run, this_run)

        repo.record(doc.serialize())
        repo.logout()

        return doc

crimes.execute()
doc = crimes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
