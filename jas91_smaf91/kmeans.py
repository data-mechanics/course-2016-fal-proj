import dml
import prov.model
import datetime
import uuid
import json
import sys

from geopy.distance import great_circle
from sklearn import cluster
from bson.code import Code

import settings as config

CODES = config.CODES
MIN_DISTANCE = config.MIN_DISTANCE
MIN_PATROLS = config.MIN_PATROLS
MAX_PATROLS = config.MAX_PATROLS

TRIAL_LIMIT = 5000

def build_query(repo):
    query = {'main_crimecode': {'$in': CODES}}
    return query

def extract_coordinates_from_crimes(repo, query, trial):
    repo.dropPermanent('jas91_smaf91.crimes_coordinates')
    repo.createPermanent('jas91_smaf91.crimes_coordinates')
    
    map_function = Code('''function() {
        id = {
            latitude: this.geo_info.geometry.coordinates[0],
            longitude: this.geo_info.geometry.coordinates[1]
        };

        if (id.latitude != 0 && id.longitude != 0) {
            emit(id,1);
        }
    }''')

    reduce_function = Code('''function(k,vs){
        return Array.sum(vs);        
    }''')

    if trial:
        repo.jas91_smaf91.crime.map_reduce(map_function, reduce_function, 'jas91_smaf91.crimes_coordinates', query=query, limit=TRIAL_LIMIT)
    else:
        repo.jas91_smaf91.crime.map_reduce(map_function, reduce_function, 'jas91_smaf91.crimes_coordinates', query=query)
        

def load_data(repo):

    data = []
    for document in repo.jas91_smaf91.crimes_coordinates.find():
        coordinates = [document['_id']['latitude'], document['_id']['longitude']]
        data.append(coordinates)

    return data

def run_kmeans(data, k):
    kmeans = cluster.KMeans(n_clusters=k)
    kmeans.fit(data)


    return kmeans.cluster_centers_, kmeans.labels_

def find_minimum_patrols(data, min_p, max_p, min_distance):

    unfeasable = False
    for patrols in range(min_p, max_p):
        unfeasable, max_distance, centers = is_feasible(data, patrols, min_distance)
        if not unfeasable:
            return patrols, None, centers

    return None, max_distance, None

def is_feasible(data, k, min_distance):

    centers, labels = run_kmeans(data, k)

    max_distance = -1
    unfeasable = False

    for i in range(len(data)):
        center = centers[labels[i]]
        distance = great_circle(data[i], center).miles

        if distance > max_distance:
            max_distance = distance

        if max_distance > min_distance:
            unfeasable = True
            break

    return unfeasable, max_distance, centers

def store_patrols_coordinates(repo, centers):
    repo.dropPermanent('jas91_smaf91.patrols_coordinates')
    repo.createPermanent('jas91_smaf91.patrols_coordinates')
    records = []
    for i in range(len(centers)):
        records.append({
            '_id': i,
            'geo_info': {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [centers[i][0], centers[i][1]]
                }
            }
        })

    repo.jas91_smaf91.patrols_coordinates.insert_many(records)

def get_crimes_coordinates(repo):
    coordinates = repo.jas91_smaf91.crimes_coordinates.find()
    return [ (c['_id']['latitude'], c['_id']['longitude']) for c in coordinates]

class kmeans(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = ['jas91_smaf91.crime']
    writes = []

    @staticmethod
    def api_execute(min_distance, min_patrols, max_patrols, codes):
        global MIN_DISTANCE
        global MIN_PATROLS
        global MAX_PATROLS
        global CODES 

        MIN_DISTANCE = int(min_distance)
        MIN_PATROLS = int(min_patrols)
        MAX_PATROLS = int(max_patrols)
        CODES = codes.split(',')
        res = kmeans.execute()
        patrols_coordinates = [(c[0],c[1]) for c in res['patrols_coordinates']] if res['patrols_coordinates'] != None  else []
        crimes_coordinates = res['crimes_coordinates']
        return patrols_coordinates,crimes_coordinates
        

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        if trial:
            print("[OUT] Running in Trial Mode")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        query = build_query(repo)

        extract_coordinates_from_crimes(repo, query, trial)

        data = load_data(repo)

        patrols, max_distance, centers = find_minimum_patrols(data, MIN_PATROLS, MAX_PATROLS, MIN_DISTANCE)

        if patrols:
            store_patrols_coordinates(repo, centers)
        else:
            repo.dropPermanent('jas91_smaf91.patrols_coordinates')

        crimes_coordinates = get_crimes_coordinates(repo)

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime, "patrols_coordinates": centers, "crimes_coordinates": crimes_coordinates}

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

        this_script = doc.agent('alg:jas91_smaf91#kmeans', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        min_patrols = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Minimun number of patrols to be allocated to minimize the distance between the patrols and crime areas'})
        doc.wasAssociatedWith(min_patrols, this_script)
        
        resource_crime = doc.entity('dat:jas91_smaf91#crime', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            min_patrols, 
            resource_crime, 
            startTime, 
            None,
            {}
        )
        
        resource_patrols_coordinates = doc.entity('dat:jas91_smaf91#patrols_coordinates', {'prov:label':'Patrol Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(resource_patrols_coordinates, this_script)
        doc.wasGeneratedBy(resource_patrols_coordinates, min_patrols, endTime)
        doc.wasDerivedFrom(resource_patrols_coordinates, resource_crime, min_patrols, min_patrols, min_patrols) 
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

if 'trial' in sys.argv:
    kmeans.execute(True)
#else:
#    kmeans.execute()

#doc = kmeans.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))

