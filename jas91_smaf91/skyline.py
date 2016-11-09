import dml
import prov.model
import datetime
import uuid
import json

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
    reads = ['jas91_smaf91.crime', 'jas91_smaf91.sr311']
    writes = ['jas91_smaf91.sr311_crime_per_zip_code']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

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
        pass

skyline.execute()
