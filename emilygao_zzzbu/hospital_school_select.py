import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



class school_hospital_s(dml.Algorithm):
    contributor = 'emilygao_zzzbu'
    reads = ['emilygao_zzzbu.hospital','emilygao_zzzbu.school']
    writes = ['emilygao_zzzbu.school_hospital_s']

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
            if document['zipcode'] == '2215':
                hospitals_array.append({"zip":document['zipcode'], 'info':{"hospitalName":document['name']}})

        schools_array = []
        for document in schools.find():
            if document['zipcode'] == '2215':
                schools_array.append({"zip":document['zipcode'],'info': {"schoolName":document['sch_name']}})

        repo.dropPermanent("school_hospital_s")
        repo.createPermanent("school_hospital_s")
        repo['emilygao_zzzbu.school_hospital_s'].insert_many(schools_array)
        repo['emilygao_zzzbu.school_hospital_s'].insert_many(hospitals_array)


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

        this_script = doc.agent('alg:emilygao_zzzbu#school_hospital_s', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'School and Hospital in 02215', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_school_hospital_s = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_school_hospital_s, this_script)
        doc.usage(get_school_hospital_s, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        school_hospital_s = doc.entity('dat:emilygao_zzzbu#school_hospital_s', {prov.model.PROV_LABEL:'School and Hospital in 02215', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(school_hospital_s, this_script)
        doc.wasGeneratedBy(school_hospital_s, get_school_hospital_s, endTime)
        doc.wasDerivedFrom(school_hospital_s, resource, get_school_hospital_s, get_school_hospital_s, get_school_hospital_s)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

school_hospital_s.execute()
doc = school_hospital_s.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
