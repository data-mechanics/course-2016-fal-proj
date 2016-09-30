import json
import dml
import prov.model
import datetime
import uuid

class transformation3(dml.Algorithm):
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

        def project(document):
            return {'geo_info': document['geo_info']}

        def union(collection1, collection2, result, f):
            for document in repo[collection1].find():
                document = f(document) 
                coordinates = document['geo_info']['geometry']['coordinates']
                if coordinates[0] and coordinates[1]:
                    repo[result].insert(f(document))

            for document in repo[collection2].find():
                document = f(document)
                coordinates = document['geo_info']['geometry']['coordinates']
                if coordinates[0] and coordinates[1]:
                    repo[result].insert(f(document))

        repo.dropPermanent('jas91_smaf91.union_temp')
        repo.createPermanent('jas91_smaf91.union_temp')
        print('[OUT] computing the union of the datasets')
        union('jas91_smaf91.sr311', 'jas91_smaf91.food', 'jas91_smaf91.union_temp', project)
        union('jas91_smaf91.schools', 'jas91_smaf91.hospitals', 'jas91_smaf91.union_temp', project)
        print('[OUT] done')

        print('[OUT] indexing by latitude and longitude')
        repo.jas91_smaf91.union_temp.ensure_index([('geo_info.geometry', dml.pymongo.GEOSPHERE)])
        print('[OUT] done')

        print('[OUT] filling crime zip codes')
        for document in repo.jas91_smaf91.crime.find():
            latitude = document['geo_info']['geometry']['coordinates'][0]
            longitude = document['geo_info']['geometry']['coordinates'][1]
            neighbor = repo.jas91_smaf91.union_temp.find_one({ 
                'geo_info.geometry': {
                    '$near': { 
                        '$geometry': {
                            'type': 'Point', 
                            'coordinates' :[latitude,longitude]
                            }, 
                        '$maxDistance': 1000, 
                        '$minDistance': 0 
                        } 
                    }
                })

            if neighbor:
                zip_code = neighbor['geo_info']['properties']['zip_code']
                document['geo_info']['properties']['zip_code'] = zip_code
                repo.jas91_smaf91.crime.update(
                        {'_id': document['_id']}, 
                        document,
                        upsert=False
                        )
        repo.logout()
        print('[OUT] done')

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass

transformation3.execute()
