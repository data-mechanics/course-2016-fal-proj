import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getGeoJSON(dml.Algorithm):
    contributor = 'bsowens_ggelinas'
    reads = ['bsowens_ggelinas.stations',
              'bsowens_ggelinas.incidents',
              'bsowens_ggelinas.property',
              'bsowens_ggelinas.fio',
              'bsowens_ggelinas.hospitals']
    writes = ['bsowens_ggelinas.stations',
              'bsowens_ggelinas.incidents',
              'bsowens_ggelinas.property',
              'bsowens_ggelinas.fio',
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
            # Dependent on "getFIOcoord.py" (must be run before this function returns anything other than None)
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
                'unset': {'coords': ''},
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

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/bsowens_ggelinas') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/bsowens_ggelinas') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bsowens_ggelinas#alg1',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                  {'prov:label': 'Standarize geographical information'})

        doc.wasAssociatedWith(this_run, this_script)

        resource_stations = doc.entity('dat:bsowens_ggelinas#stations',
                                    {'prov:label': 'Police Station Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            this_run,
            resource_stations,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_incidents = doc.entity('dat:bsowens_ggelinas#incidents',
                                    {'prov:label': 'Police Incident Reports', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            this_run,
            resource_incidents,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_property = doc.entity('dat:bsowens_ggelinas#property',
                                        {'prov:label': 'Property Values', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            this_run,
            resource_property,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_fio = doc.entity('dat:bsowens_ggelinas#fio',
                                   {'prov:label': 'Field Interrogation Observations',
                                                             prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            this_run,
            resource_fio,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_hospitals = doc.entity('dat:bsowens_ggelinas#hospitals',
                                      {'prov:label': 'Hospital Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            this_run,
            resource_hospitals,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )
        stations = doc.entity('dat:bsowens_ggelinas#stations', {prov.model.PROV_LABEL:'Stations with GeoJSON', prov.model.PROV_TYPE:'ont:DataSet'})
        incidents = doc.entity('dat:bsowens_ggelinas#incidents', {prov.model.PROV_LABEL:'Incidents with GeoJSON', prov.model.PROV_TYPE:'ont:DataSet'})
        property = doc.entity('dat:bsowens_ggelinas#property', {prov.model.PROV_LABEL:'Property with GeoJSON', prov.model.PROV_TYPE:'ont:DataSet'})
        fio = doc.entity('dat:bsowens_ggelinas#fio', {prov.model.PROV_LABEL:'FIO with GeoJSON', prov.model.PROV_TYPE:'ont:DataSet'})
        hospitals = doc.entity('dat:bsowens_ggelinas#hospitals', {prov.model.PROV_LABEL:'Hospitals with GeoJSON', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(stations, this_script)
        doc.wasAttributedTo(incidents, this_script)
        doc.wasAttributedTo(property, this_script)
        doc.wasAttributedTo(fio, this_script)
        doc.wasAttributedTo(hospitals, this_script)

        doc.wasGeneratedBy(stations, this_run, endTime)
        doc.wasGeneratedBy(incidents, this_run, endTime)
        doc.wasGeneratedBy(property, this_run, endTime)
        doc.wasGeneratedBy(fio, this_run, endTime)
        doc.wasGeneratedBy(hospitals, this_run, endTime)

        doc.wasDerivedFrom(stations, resource_stations, this_run, this_run, this_run)
        doc.wasDerivedFrom(incidents, resource_incidents, this_run, this_run, this_run)
        doc.wasDerivedFrom(property, resource_property, this_run, this_run, this_run)
        doc.wasDerivedFrom(fio, resource_fio, this_run, this_run, this_run)
        doc.wasDerivedFrom(hospitals, resource_hospitals, this_run, this_run, this_run)

        repo.record(doc.serialize())
        repo.logout()

        return doc

getGeoJSON.execute()
doc = getGeoJSON.provenance()
print (doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

