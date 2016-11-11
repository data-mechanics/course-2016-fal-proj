import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

TRIAL_LIMIT = 5000

class transformation1(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = ['jas91_smaf91.crime', 'jas91_smaf91.311', 'jas91_smaf91.hospitals', 'jas91_smaf91.food', 'jas91_smaf91.schools']
    writes = ['jas91_smaf91.crime', 'jas91_smaf91.311', 'jas91_smaf91.hospitals', 'jas91_smaf91.food', 'jas91_smaf91.schools']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        if trial:
            print("[OUT] Running in Trial Mode")

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
                    'zip_code': to_int(zip_code)
                }
            }

        def to_int(x):
            try:
                return int(x)
            except:
                return None

        def to_float(x):
            try:
                return float(x)
            except:
                return None

        def get_geo_info_food(entry):
            zip_code = entry['zip'] if 'zip' in entry else None
            latitude = entry['location']['coordinates'][1] if 'location' in entry else None
            latitude = to_float(latitude)
            longitude = entry['location']['coordinates'][0] if 'location' in entry else None
            longitude = to_float(longitude)
            return zip_code, latitude, longitude

        def get_geo_info_schools(entry):
            zip_code = entry['zip_code'] if 'zip_code' in entry else None
            latitude = entry['map_location']['coordinates'][1] if 'map_location' in entry else None
            latitude = to_float(latitude)
            longitude = entry['map_location']['coordinates'][0] if 'map_location' in entry else None
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
            latitude = entry['geocoded_location']['coordinates'][1] if 'geocoded_location' in entry else None
            latitude = to_float(latitude)
            longitude = entry['geocoded_location']['coordinates'][0] if 'geocoded_location' in entry else None
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

        if trial:
            limit = TRIAL_LIMIT
        else:
            limit = ""

        for collection_id in collections:
            collection = collections[collection_id]
            for document in repo[collection['name']].find().limit(limit):
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

        this_script = doc.agent('alg:jas91_smaf91#transformation1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        standarize = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label': 'Standarize geographical information'})
        doc.wasAssociatedWith(standarize, this_script)

        resource_crime = doc.entity('dat:jas91_smaf91#crime', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            standarize, 
            resource_crime, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )

        resource_sr311 = doc.entity('dat:jas91_smaf91#sr311', {'prov:label':'311 Service Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            standarize, 
            resource_sr311, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )

        resource_hospitals = doc.entity('dat:jas91_smaf91#hospitals', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            standarize, 
            resource_hospitals, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )

        resource_food = doc.entity('dat:jas91_smaf91#food', {'prov:label':'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            standarize, 
            resource_food, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )

        resource_schools = doc.entity('dat:jas91_smaf91#schools', {'prov:label':'Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            standarize, 
            resource_schools, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )

        doc.wasAttributedTo(resource_crime, this_script)
        doc.wasAttributedTo(resource_sr311, this_script)
        doc.wasAttributedTo(resource_hospitals, this_script)
        doc.wasAttributedTo(resource_food, this_script)
        doc.wasAttributedTo(resource_schools, this_script)

        doc.wasGeneratedBy(resource_crime, standarize, endTime)
        doc.wasGeneratedBy(resource_sr311, standarize, endTime)
        doc.wasGeneratedBy(resource_hospitals, standarize, endTime)
        doc.wasGeneratedBy(resource_food, standarize, endTime)
        doc.wasGeneratedBy(resource_schools, standarize, endTime)

        doc.wasDerivedFrom(resource_sr311, resource_sr311, standarize, standarize, standarize) 
        doc.wasDerivedFrom(resource_hospitals, resource_hospitals, standarize, standarize, standarize) 
        doc.wasDerivedFrom(resource_food, resource_food, standarize, standarize, standarize) 
        doc.wasDerivedFrom(resource_schools, resource_schools, standarize, standarize, standarize) 
        doc.wasDerivedFrom(resource_crime, resource_crime, standarize, standarize, standarize) 

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

transformation1.execute(True)
#doc = transformation1.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))
