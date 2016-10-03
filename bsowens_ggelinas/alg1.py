import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class algorithm1(dml.Algorithm):
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
                    'coordinates': [latitude, longitude]
                },
                'properties': {
                    'zip_code': int(zip_code)
                }
            }
