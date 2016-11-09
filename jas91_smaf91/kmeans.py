import dml
import prov.model
import datetime
import uuid
import json

from geopy.distance import great_circle
from sklearn import cluster
from bson.code import Code

import settings as config

CODES = config.CODES
MIN_DISTANCE = config.MIN_DISTANCE
MIN_PATROLS = config.MIN_PATROLS
MAX_PATROLS = config.MAX_PATROLS

def build_query():
    query = {'main_crimecode': {'$in': CODES}}
    return query

def extract_coordinates_from_crimes(repo, query):
    repo.dropPermanent('jas91_smaf91.crime_coordinates')
    repo.createPermanent('jas91_smaf91.crime_coordinates')
    
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

    repo.jas91_smaf91.crime.map_reduce(map_function, reduce_function, 'jas91_smaf91.crime_coordinates', query=query)
        
    print('[OUT] done extracting the coordinates')

def load_data(repo):

    data = []
    for document in repo.jas91_smaf91.crime_coordinates.find():
        coordinates = [document['_id']['latitude'], document['_id']['longitude']]
        data.append(coordinates)

    return data

def run_kmeans(data, k):
    kmeans = cluster.KMeans(n_clusters=k)
    kmeans.fit(data)

    print('[OUT] done running kmeans++')

    return kmeans.cluster_centers_, kmeans.labels_

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

    return unfeasable

class kmeans(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = []
    writes = []

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        query = build_query()

        extract_coordinates_from_crimes(repo, query)

        data = load_data(repo)

        unfeasable = is_feasible(data, MIN_PATROLS, MIN_DISTANCE)

        if unfeasable:
            print('It is not feasible to locate patrols with a range less than', min_distance, 'miles.')
            print('At least one zone is left with a distance of', max_distance, '.', 'consider increasing the number of patrols or the minimum distance.')
        else:
            print('Placing', k, 'patrols at a minimum distance of', min_distance, 'miles from high crime rate zones is feasable.')

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

kmeans.execute()

