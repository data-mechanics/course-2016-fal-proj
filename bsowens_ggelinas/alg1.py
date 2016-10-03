import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class alg1(dml.Algorithm):
    contributor = 'bsowens_ggelinas'
    reads = ['bsowens_ggelinas.stations',
              'bsowens_ggelinas.incidents',
              'bsowens_ggelinas.property',
              'bsoquitwens_ggelinas.fio',
              'bsowens_ggelinas.hospitals']
    writes = ['bsowens_ggelinas.stations',
              'bsowens_ggelinas.incidents',
              'bsowens_ggelinas.property',
              'bsoquitwens_ggelinas.fio',
              'bsowens_ggelinas.hospitals']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bsowens_ggelinas', 'bsowens_ggelinas')

        def encode_geojson(file,element):
            zip,lat,long, = file(element)
            return {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lat, long]
                },
                'properties': {
                    'zip_code': myint(zip)
                }
            }


        def myint(num):
            try:
                return int(num)
            except:
                return None

        def myfloat(num):
            try:
                return float(num)
            except:
                return None

        '''The following functions retrieve the zipcode (where available) and coordinates from each dataset'''

        def get_loc_stations(item):
            zip = item["location_zip"]
            lat = item['location']['coordinates'][1] if 'location' in item else None
            lat = myfloat(lat)
            long = item['location']['coordinates'][0] if 'location' in item else None
            long = myfloat(long)
            return zip,lat,long

        def get_loc_incidents(item):
            zip = None
            lat = item['lat'] if 'lat' in item else None
            lat = myfloat(lat)
            long = item['long'] if 'long' in item else None
            long = myfloat(long)
            return zip, lat, long

        def get_loc_property(item):
            zip = item['mail_zipcode']
            lat = item['latitude']
            lat = myfloat(lat)
            long = item['longitude']
            long = myfloat(long)
            return zip, lat, long

        def get_loc_fio(item):
            zip = None
            lat = item['coords']['lat'] if 'coords' in item else None
            lat = myfloat(lat)
            long = item['coords']['lng'] if 'coords' in item else None
            long = myfloat(long)
            return zip, lat, long

        def get_loc_hospitals(item):
            zip = item['location_zip']
            lat = item['location']['coordinates'][1]
            lat = myfloat(lat)
            long = item['location']['coordinates'][0]
            long = myfloat(long)
            return zip, lat, long


        collections = {
            'stations': {
                'name': 'bsowens_ggelinas.stations',
                'unset': {'location': '', 'location_zip': ''},
                'loc': get_loc_stations
            },
            'incidents': {
                'name': 'bsowens_ggelinas.incidents',
                'unset': {'lat': '', 'long': ''},
                'loc': get_loc_incidents
            },
            'property': {
                'name': 'bsowens_ggelinas.property',
                'unset': {'mail_zipcode': '', 'latitude': '', 'longitude': ''},
                'loc': get_loc_property
            },
            'fio': {
                'name': 'bsowens_ggelinas.fio',
                'unset': {},
                'loc': get_loc_fio
            },
            'hospitals': {
                'name': 'bsowens_ggelinas.hospitals',
                'unset': {'location_zip': '', 'location': ''},
                'loc': get_loc_hospitals
            }
        }

        for collection_name in collections:
            collection = collections[collection_name]
            print('Status: Processing collection: ', collection_name)
            for doc in repo[collection['name']].find():
                if 'loc_info' in doc:
                    continue
                geojson = encode_geojson(collection['loc'],doc)
                repo[collection['name']].update(
                    {'_id': doc['_id']},
                    {
                        '$set': {'loc_info': geojson},
                        '$unset': collection['unset']
                    },
                    upsert=False
                )
            print('Status: Completed collection: ', collection_name)
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}





    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bsowens_ggelinas', 'bsowens_ggelinas')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        # The scripts are in <folder>#<filename> format.

        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        # The data sets are in <user>#<collection> format.

        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.

        doc.add_namespace('log', 'http://datamechanics.io/log/')
        # The event log.

        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bsowens_ggelinas#transformation1',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        standarize = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                  {'prov:label': 'Standarize geographical information'})
        doc.wasAssociatedWith(standarize, this_script)

        resource_stations = doc.entity('dat:bsowens_ggelinas#stations',
                                    {'prov:label': 'Police Station Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            standarize,
            resource_stations,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_incidents = doc.entity('dat:bsowens_ggelinas#incidents',
                                    {'prov:label': 'Police Incident Reports', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            standarize,
            resource_incidents,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_property = doc.entity('dat:bsowens_ggelinas#property',
                                        {'prov:label': 'Property Values', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            standarize,
            resource_property,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_fio = doc.entity('dat:bsowens_ggelinas#fio',
                                   {'prov:label': 'Field Interrogation Observations',
                                                             prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            standarize,
            resource_fio,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_hospitals = doc.entity('dat:bsowens_ggelinas#hospitals',
                                      {'prov:label': 'Hospital Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            standarize,
            resource_hospitals,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        doc.wasAttributedTo(resource_stations, this_script)
        doc.wasAttributedTo(resource_incidents, this_script)
        doc.wasAttributedTo(resource_property, this_script)
        doc.wasAttributedTo(resource_fio, this_script)
        doc.wasAttributedTo(resource_hospitals, this_script)

        doc.wasGeneratedBy(resource_stations, standarize, endTime)
        doc.wasGeneratedBy(resource_incidents, standarize, endTime)
        doc.wasGeneratedBy(resource_property, standarize, endTime)
        doc.wasGeneratedBy(resource_fio, standarize, endTime)
        doc.wasGeneratedBy(resource_hospitals, standarize, endTime)

        doc.wasDerivedFrom(resource_stations, resource_stations, standarize, standarize, standarize)
        doc.wasDerivedFrom(resource_incidents, resource_incidents, standarize, standarize, standarize)
        doc.wasDerivedFrom(resource_property, resource_property, standarize, standarize, standarize)
        doc.wasDerivedFrom(resource_fio, resource_fio, standarize, standarize, standarize)
        doc.wasDerivedFrom(resource_hospitals, resource_hospitals, standarize, standarize, standarize)

        repo.record(doc.serialize())
        repo.logout()

        return doc

alg1.execute()
doc = alg1.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))