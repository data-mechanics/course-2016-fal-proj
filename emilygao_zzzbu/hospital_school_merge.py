import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



class school_hospital(dml.Algorithm):
    contributor = 'emilygao_zzzbu'
    reads = ['emilygao_zzzbu.hospital','emilygao_zzzbu.school']
    writes = ['emilygao_zzzbu.school_hospital']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('emilygao_zzzbu', 'emilygao_zzzbu')
        
        schools = repo['emilygao_zzzbu.school']
        hospitals = repo['emilygao_zzzbu.hospital']
        
        hospitals_array = []
        for document in hospitals.find():
            hospitals_array.append({"zip":document['zipcode'], 'info':{"hospitalName":document['name']}})

        schools_array = []
        for document in schools.find():
            schools_array.append({"zip":document['zipcode'], "info":{"schoolName":document['sch_name']}})

        repo.dropPermanent("school_hospital")
        repo.createPermanent("school_hospital")
        repo['emilygao_zzzbu.school_hospital'].insert_many(schools_array)
        repo['emilygao_zzzbu.school_hospital'].insert_many(hospitals_array)


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

        this_script = doc.agent('alg:emilygao_zzzbu#school_hospital', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'School and Hospital', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_school_hospital = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_school_hospital, this_script)
        doc.usage(get_school_hospital, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        school_hospital = doc.entity('dat:emilygao_zzzbu#school_hospital', {prov.model.PROV_LABEL:'School and Hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(school_hospital, this_script)
        doc.wasGeneratedBy(school_hospital, get_school_hospital, endTime)
        doc.wasDerivedFrom(school_hospital, resource, get_school_hospital, get_school_hospital, get_school_hospital)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

school_hospital.execute()
doc = school_hospital.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
