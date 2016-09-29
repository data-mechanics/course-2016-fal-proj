import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class transformation1(dml.Algorithm):
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

        def get_geojson(f, entry):
            zip_code, latitude, longitude = f(entry)
            return {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [latitude, longitude]
                },
                'properties': {
                    'zip_code': zip_code
                }
            }

        def to_float(x):
            try:
                return float(x)
            except:
                return None

        def get_geo_info_food(entry):
            zip_code = entry['zip'] if 'zip' in entry else None
            latitude = entry['location']['coordinates'][0] if 'location' in entry else None
            latitude = to_float(latitude)
            longitude = entry['location']['coordinates'][1] if 'location' in entry else None
            longitude = to_float(longitude)
            return zip_code, latitude, longitude

        def get_geo_info_schools(entry):
            zip_code = entry['zip_code'] if 'zip_code' in entry else None
            latitude = entry['map_location']['coordinates'][0] if 'map_location' in entry else None
            latitude = to_float(latitude)
            longitude = entry['map_location']['coordinates'][1] if 'map_location' in entry else None
            longitude = to_float(longitude)
            return zip_code, latitude, longitude

        def get_geo_info_hospitals(entry):
            zip_code = entry['zipcode'] if 'zipcode' in entry else None
            latitude = entry['location']['coordinates'][1] if 'location' in entry else None
            latitude = to_float(latitude)
            longitude = entry['location']['coordinates'][0] if 'location' in entry else None
            longitude = to_float(longitude)
            return zip_code, latitude, longitude

        def get_geo_info_crime(entry):
            zip_code = None
            latitude = entry['location']['coordinates'][1] if 'location' in entry else None
            latitude = to_float(latitude)
            longitude = entry['location']['coordinates'][0] if 'location' in entry else None
            longitude = to_float(longitude)
            return zip_code, latitude, longitude

        def get_geo_info_sr311(entry):
            zip_code = entry['location_zipcode'] if 'location_zipcode' in entry else None
            latitude = entry['geocoded_location']['latitude'][1] if 'geocoded_location' in entry else None
            latitude = to_float(latitude)
            longitude = entry['geocoded_location']['longitude'][0] if 'geocoded_location' in entry else None
            longitude = to_float(longitude)
            return zip_code, latitude, longitude

        collections = {
            'food': {
                'name': 'jas91_smaf91.food',
                'unset': {'location':'', 'zip':''},
                'get_geo_info': get_geo_info_food
            },
            'schools': {
                'name': 'jas91_smaf91.schools',
                'unset': {'map_location':'', 'map_locations':'', 'zip_code':'', 'coordinates':''},
                'get_geo_info': get_geo_info_schools
            },
            'hospitals': {
                'name': 'jas91_smaf91.hospitals',
                'unset': {'zipcode':'', 'location':'', 'location_zip':'', 'xcoord':'', 'ycoord':''},
                'get_geo_info': get_geo_info_hospitals
            },
            'crime': {
                'name': 'jas91_smaf91.crime',
                'unset': {'location':''},
                'get_geo_info': get_geo_info_crime
            },
            'sr311': {
                'name': 'jas91_smaf91.sr311',
                'unset': {'location_zipcode':'', 'geocoded_location':'', 'location_x':'', 'location_y':''},
                'get_geo_info': get_geo_info_sr311
            }
        }

        for collection_id in collections:
            collection = collections[collection_id]
            for document in repo[collection['name']].find():
                if 'geo_info' in document:
                    continue

                geojson = get_geojson(collection['get_geo_info'], document)
                repo[collection['name']].update(
                    {'_id': document['_id']}, 
                    {
                        '$set': {'geo_info': geojson},
                        '$unset': collection['unset'] 
                    },
                    upsert=False
                )
            print('[OUT] done with', collection_id)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

transformation1.execute()
