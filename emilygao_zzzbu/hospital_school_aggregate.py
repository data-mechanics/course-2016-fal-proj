import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



class school_hospital_a(dml.Algorithm):
    contributor = 'emilygao_zzzbu'
    reads = ['emilygao_zzzbu.hospital','emilygao_zzzbu.school']
    writes = ['emilygao_zzzbu.school_hospital_a']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        schools = repo['emilygao_zzzbu.school']
        hospitals = repo['emilygao_zzzbu.hospital']
        
        zips = []
        for document in hospitals.find():
            zips.append((document['zipcode'], 1))

        for document in schools.find():
            zips.append((document['zipcode'], 1))
        keys = {r[0] for r in zips}
        zips = [(key, sum([v for (k,v) in zips if k == key])) for key in keys]

        insert = []
        for x in zips:
            insert.append({"zip":x[0],'info': {"numberOfSchoolAndHospital":x[1]}})
            

        repo.dropPermanent("school_hospital_a")
        repo.createPermanent("school_hospital_a")
        repo['emilygao_zzzbu.school_hospital_a'].insert_many(insert)



        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emilygao_zzzbu', 'emilygao_zzzbu')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:emilygao_zzzbu#school_hospital_a', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'School and Hospital Number in each zipcode', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_school_hospital_a = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_school_hospital_a, this_script)
        doc.usage(get_school_hospital_a, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        school_hospital_a = doc.entity('dat:emilygao_zzzbu#school_hospital_a', {prov.model.PROV_LABEL:'School and Hospital number in each zipcode', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(school_hospital_a, this_script)
        doc.wasGeneratedBy(school_hospital_a, get_school_hospital_a, endTime)
        doc.wasDerivedFrom(school_hospital_a, resource, get_school_hospital_a, get_school_hospital_a, get_school_hospital_a)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

school_hospital_a.execute()
doc = school_hospital_a.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
