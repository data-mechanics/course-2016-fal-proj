import urllib.request
import urllib.parse
import json
import dml
import prov.model
import datetime
import uuid

class numOfCrimeInDistrics(dml.Algorithm):
    contributor = 'bsowens_ggelinas'
    reads = ['bsowens_ggelinas.stations',
             'bsowens_ggelinas.incidents',
             'bsowens_ggelinas.property',
             'bsoquitwens_ggelinas.fio',
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
