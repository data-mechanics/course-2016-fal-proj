import urllib.request
import urllib.parse
import json
import dml
import prov.model
import datetime
import uuid


class numOfCrimeInDistricts(dml.Algorithm):
    contributor = 'bsowens_ggelinas'
    reads = ['bsowens_ggelinas.stations',
              'bsowens_ggelinas.incidents']
    writes = ['bsowens_ggelinas.stations',
              'bsowens_ggelinas.incidents']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bsowens_ggelinas', 'bsowens_ggelinas')


        repo['bsowens_ggelinas.stations'].remove({"name": "Boston Police Headquarters"})

        # We had to hard code the list of districts because the names
        # were not consistent in the dataset. If we use it in the future
        # we will make it actually pull these from the dataset.

        districts = ['A1', 'D4', 'E13', 'B3', 'E18', 'D14', 'A7', 'C6', 'B2', 'E5', 'C11']
        counter = 0
        for doc in repo['bsowens_ggelinas.stations'].find():
            district = districts[counter]
            num_crimes = repo['bsowens_ggelinas.incidents'].count({'district': district})
            print(num_crimes)

            repo['bsowens_ggelinas.stations'].update(
                {'_id': doc['_id']},
                {'$set': {'num_crimes': num_crimes},},
                upsert=False
            )
            counter += 1

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}\

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
        repo.authenticate('bsowens_ggelinas', 'bsowens_ggelinas')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/bsowens_ggelinas') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/bsowens_ggelinas')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bsowens_ggelinas#numOfCrimeInDistricts', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        stations_info = doc.entity('bdp:pyxn-r3i2', {'prov:label':'District Police Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        stations_getInfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get District Police Stations Data'})
        doc.wasAssociatedWith(stations_getInfo, this_script)
        doc.usage(
            stations_getInfo,
            stations_info,
            startTime,
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )

        incidents_info = doc.entity('bdp:29yf-ye7n',
                                   {'prov:label': 'Crime Incidents Report', prov.model.PROV_TYPE: 'ont:DataResource',
                                    'ont:Extension': 'json'})
        incidents_getInfo = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                        {'prov:label': 'Get Crime Incidents Report Data'})
        doc.wasAssociatedWith(incidents_getInfo, this_script)
        doc.usage(
            incidents_getInfo,
            incidents_info,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Retrieval'}
        )

        incidents = doc.entity('dat:bsowens_ggelinas#incidents',
                              {prov.model.PROV_LABEL: 'Crime Incidents Report', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(incidents, this_script)
        doc.wasGeneratedBy(incidents, incidents_getInfo, endTime)

        property = doc.entity('dat:bsowens_ggelinas#property',
                         {prov.model.PROV_LABEL: 'Property Assessment 2016', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(property, this_script)
        doc.wasGeneratedBy(property, property_getInfo, endTime)

        fio = doc.entity('dat:bsowens_ggelinas#fio',
                               {prov.model.PROV_LABEL: 'Boston Police Department FIO', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(fio, this_script)
        doc.wasGeneratedBy(fio, fio_getInfo, endTime)

        hospitals = doc.entity('dat:bsowens_ggelinas#hospitals', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospitals, this_script)
        doc.wasGeneratedBy(hospitals, hospitals_getInfo, endTime)

        repo.record(doc.serialize())
        repo.logout()

        return doc

numOfCrimeInDistricts.execute()
doc = numOfCrimeInDistricts.provenance()
print(json.dumps(json.loads(doc.serialize()),indent=4))

