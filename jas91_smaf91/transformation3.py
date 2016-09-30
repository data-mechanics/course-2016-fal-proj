import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import os

class transformation3(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = ['sr311', 'food', 'schools', 'hospitals']
    writes = ['crime']

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
        for document in repo.jas91_smaf91.union_temp.find():
            latitude = document['geo_info']['geometry']['coordinates'][0]
            longitude = document['geo_info']['geometry']['coordinates'][1]
            neighbor = repo.jas91_smaf91.union_temp.find_one({ 
                'geo_info.geometry': {
                    '$near': { 
                        '$geometry': {
                            'type': 'Point', 
                            'coordinates' :[latitude,longitude]
                            }, 
                        '$maxDistance': 100, 
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

        this_script = doc.agent('alg:jas91_smaf91#transformation3', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        populate = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Populate crime zip code field'})
        doc.wasAssociatedWith(populate, this_script)

        # THESE WERE QUERIED ON THE CONDITION THAT COORDINATES ARE NOT NULL
        # ADD TO PROVENANCE
        # ADD TO THE ACTUAL QUERY
        resource_sr311 = doc.entity('dat:jas91_smaf91#sr311', {'prov:label':'311 Service Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            populate, 
            resource_sr311, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Query'}
        )

        resource_hospitals = doc.entity('dat:jas91_smaf91#hospitals', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            populate, 
            resource_hospitals, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Query'}
        )

        resource_food = doc.entity('dat:jas91_smaf91#food', {'prov:label':'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            populate, 
            resource_food, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Query'}
        )

        resource_schools = doc.entity('dat:jas91_smaf91#schools', {'prov:label':'Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            populate, 
            resource_schools, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Query'}
        )

        crime = doc.entity('dat:jas91_smaf91#crime', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            populate, 
            crime, 
            startTime, 
            None,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )

        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, populate, endTime)
        doc.wasDerivedFrom(crime, resource_sr311, populate, populate, populate) 
        doc.wasDerivedFrom(crime, resource_hospitals, populate, populate, populate) 
        doc.wasDerivedFrom(crime, resource_food, populate, populate, populate) 
        doc.wasDerivedFrom(crime, resource_schools, populate, populate, populate) 
        doc.wasDerivedFrom(crime, crime, populate, populate, populate) 

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

#transformation3.execute()
doc = transformation3.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))
