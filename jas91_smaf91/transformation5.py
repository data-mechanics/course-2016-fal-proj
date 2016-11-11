import dml
import prov.model
import datetime
import uuid
import json

from bson.code import Code

RESIDENTIAL =  ['R1','R2','R3','R4','A','RL','CD','CM']

def filter_residential(repo):
    repo.dropPermanent('jas91_smaf91.residences')
    repo.createPermanent('jas91_smaf91.residences')

    for document in repo.jas91_smaf91.address_list.find({'land_usage': {'$in': RESIDENTIAL}}):
        repo['jas91_smaf91.residences'].insert_one(document)

    #repo.dropPermanent('jas91_smaf91.address_list')


class transformation5(dml.Algorithm):
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

        filter_residential(repo)

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


transformation5.execute()
