import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class get_data(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = []
    writes = ['jas91_smaf91.crime', 'jas91_smaf91.311', 'jas91_smaf91.hospitals', 'jas91_smaf91.food', 'jas91_smaf91.schools']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        city_of_boston_datasets = {
                'crime': 'https://data.cityofboston.gov/resource/ufcx-3fdn.json',
                'sr311': 'https://data.cityofboston.gov/resource/rtbk-4hc4.json',
                'hospitals': 'https://data.cityofboston.gov/resource/u6fv-m8v4.json',
                'food': 'https://data.cityofboston.gov/resource/427a-3cn5.json',
                'schools': 'https://data.cityofboston.gov/resource/pzcy-jpz4.json'
                }

        for dataset in city_of_boston_datasets:
            response = urllib.request.urlopen(city_of_boston_datasets[dataset]).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            repo.dropPermanent(dataset)
            repo.createPermanent(dataset)
            repo['jas91_smaf91.' + dataset].insert_many(r)
            print('[OUT] Done loading dataset', dataset)

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
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jas91_smaf91#get_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_crime = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Crime Incident Report Data'})
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(
            get_crime, 
            resource_crime, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )

        resource_sr311 = doc.entity('bdp:rtbk-4hc4', {'prov:label':'311 Service Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_sr311 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label': 'Get 311 Service Reports'})
        doc.wasAssociatedWith(get_sr311, this_script)
        doc.usage(
            get_sr311, 
            resource_sr311, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )

        resource_hospitals = doc.entity('bdp:u6fv-m8v4', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_hospitals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label': 'Get Hospital Locations'})
        doc.wasAssociatedWith(get_hospitals, this_script)
        doc.usage(
            get_hospitals, 
            resource_hospitals, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )

        resource_food = doc.entity('bdp:427a-3cn5', {'prov:label':'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_food = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label': 'Get Food Establishment Inspections'})
        doc.wasAssociatedWith(get_food, this_script)
        doc.usage(
            get_food, 
            resource_food, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )

        resource_schools = doc.entity('bdp:pzcy-jpz4', {'prov:label':'Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label': 'Get Schools'})
        doc.wasAssociatedWith(get_schools, this_script)
        doc.usage(
            get_schools, 
            resource_schools, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )

        crime = doc.entity('dat:jas91_smaf91#crime', {prov.model.PROV_LABEL:'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)

        sr311 = doc.entity('dat:jas91_smaf91#sr311', {prov.model.PROV_LABEL:'311 Service Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(sr311, this_script)
        doc.wasGeneratedBy(sr311, get_sr311, endTime)

        hospitals = doc.entity('dat:jas91_smaf91#hospitals', {prov.model.PROV_LABEL:'Hospital Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospitals, this_script)
        doc.wasGeneratedBy(hospitals, get_hospitals, endTime)

        food = doc.entity('dat:jas91_smaf91#food', {prov.model.PROV_LABEL:'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(food, this_script)
        doc.wasGeneratedBy(food, get_food, endTime)

        schools = doc.entity('dat:jas91_smaf91#schools', {prov.model.PROV_LABEL:'Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

# REMEMBER TO COMMENT THIS BEFORE SUBMITTING
'''
get_data.execute()
doc = get_data.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
