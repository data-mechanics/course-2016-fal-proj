import dml
import prov.model
import datetime
import uuid
import json
import sys

from bson.code import Code

DIMENSIONS = 5
DIRECTIVES = {
    0: 'MAX', # Hospitals
    1: 'MAX', # Schools
    2: 'MAX', # Inspections
    3: 'MIN', # Crimes
    4: 'MIN'  # 311 reports
}
SKYLINE = {}

def is_dominated(element1, element2):
    is_dominated = True
    is_equal     = True
    index        = 0

    while is_dominated and index < DIMENSIONS:
        if DIRECTIVES[index] == 'MAX':
            is_dominated = is_dominated and element1[index] >= element2[index]
        else:
            is_dominated = is_dominated and element1[index] <= element2[index]
        is_equal = is_equal and element1[index] == element2[index]
        index = index + 1

    if is_equal:
        return False
    else:
        return is_dominated

def generate_data(repo):
    for document in repo['jas91_smaf91.attributes_per_zip_code'].find():
        attributes = [
            document['value']['hospitals'],
            document['value']['schools'],
            round(document['value']['inspections'],2),
            document['value']['crimes'],
            document['value']['sr311']
        ]
        zip_code = document['_id']
        if zip_code:
            zip_code = int(zip_code)
            yield (zip_code, attributes)

def find_skyline_set(repo):
    
    for zip_code, att in generate_data(repo):
        dominated = False
        delete_from_skyline = {}
        for s in SKYLINE:
            if is_dominated(att, SKYLINE[s]):
                # print(att, 'dominates', SKYLINE[s])
                delete_from_skyline[s] = s
            else:
                if  is_dominated(SKYLINE[s], att):
                    # print(SKYLINE[s], 'dominates', att)
                    dominated = True
                    break

        if not dominated:
            SKYLINE[zip_code] = att

        for element in delete_from_skyline:
            del SKYLINE[element]
    
    print('[OUT] done calculating skyline')

def store_skyline_database(repo):
    repo.dropPermanent('jas91_smaf91.zip_code_skyline')
    repo.createPermanent('jas91_smaf91.zip_code_skyline')
    records = []
    for s in SKYLINE:
        hospitals = SKYLINE[s][0]
        schools = SKYLINE[s][1]
        inspections = SKYLINE[s][2]
        crimes = SKYLINE[s][3]
        sr311 = SKYLINE[s][4]
        records.append({
            '_id': s,
            'hospitals': hospitals, 
            'schools': schools, 
            'inspections': inspections, 
            'crimes': crimes, 
            'sr311': sr311
        })

    repo.jas91_smaf91.zip_code_skyline.insert_many(records)

    print('[OUT] done storing skyline')

class skyline(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = ['jas91_smaf91.attributes_per_zip_code']
    writes = ['jas91_smaf91.zip_code_skyline']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        if trial:
            print("[OUT] Running in Trial Mode")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        find_skyline_set(repo)
    
        store_skyline_database(repo)

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
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jas91_smaf91#skyline', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Computing Best zipcodes based on skyline techniques'})
        
        doc.wasAssociatedWith(run, this_script)

        resource_hospitals_per_zip_code = doc.entity('dat:jas91_smaf91#hospitals_per_zip_code', {'prov:label':'Hospitals per zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_schools_per_zip_code = doc.entity('dat:jas91_smaf91#schools_per_zip_code', {'prov:label':'Schools per zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_inspections_per_zip_code = doc.entity('dat:jas91_smaf91#inspections_per_zip_code', {'prov:label':'Food Inspections per zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_crime_per_zip_code = doc.entity('dat:jas91_smaf91#crime_per_zip_code', {'prov:label':'Crime per zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_sr311_per_zip_code = doc.entity('dat:jas91_smaf91#sr311_per_zip_code', {'prov:label':'311 requests per zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_skyline = doc.entity('dat:jas91_smaf91#skyline', {'prov:label':'Skyline set', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.usage(run, resource_hospitals_per_zip_code, startTime, None, {})
        doc.usage(run, resource_schools_per_zip_code, startTime, None, {})
        doc.usage(run, resource_inspections_per_zip_code, startTime, None, {})
        doc.usage(run, resource_crime_per_zip_code, startTime, None, {})
        doc.usage(run, resource_sr311_per_zip_code, startTime, None, {})

        doc.wasGeneratedBy(resource_skyline, run, endTime)
        doc.wasAttributedTo(resource_skyline, this_script)
        
        doc.wasDerivedFrom(resource_skyline, resource_hospitals_per_zip_code, run, run, run) 
        doc.wasDerivedFrom(resource_skyline, resource_schools_per_zip_code, run, run, run) 
        doc.wasDerivedFrom(resource_skyline, resource_inspections_per_zip_code, run, run, run) 
        doc.wasDerivedFrom(resource_skyline, resource_crime_per_zip_code, run, run, run) 
        doc.wasDerivedFrom(resource_skyline, resource_sr311_per_zip_code, run, run, run) 
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

if 'trial' in sys.argv:
    skyline.execute(True)
#else:
#    skyline.execute()
#doc = skyline.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))
