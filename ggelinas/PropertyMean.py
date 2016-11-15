import urllib.request
import urllib.parse
import json
import dml
import prov.model
import datetime
import uuid


class PropertyMean(dml.Algorithm):
    contributor = 'ggelinas'
    reads = ['ggelinas.property',
             'ggelinas.stations']
    writes = ['ggelinas.propertymean']

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k, v) in R if k == key])) for key in keys]

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]

    def product(R, S):
        return [(t, u) for t in R for u in S]

    @staticmethod
    def execute(trial = False):
        print('here')

        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ggelinas', 'ggelinas')


        #repo['ggelinas.stations'].remove({"name": "Boston Police Headquarters"})

        # We had to hard code the list of districts because the names
        # were not consistent in the dataset. If we use it in the future
        # we will make it actually pull these from the dataset.


        propData = []
        for prop in repo['ggelinas.property'].find():
            propData.append(prop)

        zipval = [(c['mail_zipcode'], (int(c['av_total']))) for c in propData]
        totalPropVal = PropertyMean.aggregate(zipval, sum)
        zipcount = [(c['mail_zipcode'], 1) for c in propData]
        totalZipCount = PropertyMean.aggregate(zipcount, sum)
        print(totalPropVal)
        print(totalZipCount)

        P = PropertyMean.product(totalPropVal, totalZipCount)
        S = PropertyMean.select(P, lambda t: t[0][0] == t[1][0])
        CV = PropertyMean.project(S, lambda t: (t[0][0], t[0][1], t[1][1]))
        print(CV)
        AvgValueZip = [(i[0], (i[1]/i[2])) for i in CV]
        print(AvgValueZip)

        repo.dropPermanent("propertymean")
        repo.createPermanent("propertymean")

        for i in AvgValueZip:
            repo['ggelinas.propertymean'].insert({'zip_code': i[0], 'avg_value': i[1]})
        
        #districts = ['A1', 'D4', 'E13', 'B3', 'E18', 'D14', 'A7', 'C6', 'B2', 'E5', 'C11']
        #counter = 0
        #for doc in repo['ggelinas.stations'].find():
        #    district = districts[counter]
        #     num_crimes = repo['ggelinas.incidents'].count({'district': district})
        #     print(num_crimes)
        #
        #     repo['ggelinas.stations'].update(
        #         {'_id': doc['_id']},
        #         {'$set': {'num_crimes': num_crimes},},
        #         upsert=False
        #     )
        #     counter += 1

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ggelinas', 'ggelinas')

        doc.add_namespace('alg',
                          'http://datamechanics.io/algorithm/ggelinas')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat',
                          'http://datamechanics.io/data/ggelinas')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ggelinas#numOfCrimeInDistricts',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        stations_resource = doc.entity('dat:ggelinas#stations', {'prov:label': 'Boston Police Stations District',
                                                                 prov.model.PROV_TYPE: 'ont:DataSet'})
        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime,
                                {'prov:label': 'Get Boston Police Stations District'})
        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(
            this_run,
            stations_resource,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Retrieval'}
        )

        incidents_resource = doc.entity('dat:ggelinas#incidents', {'prov:label': 'Crime Incidents Report',
                                                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                                                   'ont:Extension': 'json'})
        this_run2 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                 {'prov:label': 'Get Crime Incidents District Report Data'})
        doc.wasAssociatedWith(this_run2, this_script)
        doc.usage(
            this_run2,
            incidents_resource,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        stations = doc.entity('dat:ggelinas#stations',
                              {prov.model.PROV_LABEL: 'Districts incident count', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, this_run, endTime)
        doc.wasDerivedFrom(stations, stations_resource, this_run, this_run, this_run)

        incidents = doc.entity('dat:ggelinas#incidents',
                               {prov.model.PROV_LABEL: 'Counted incidents', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(incidents, this_script)
        doc.wasGeneratedBy(incidents, this_run2, endTime)
        doc.wasDerivedFrom(incidents, incidents_resource, this_run2, this_run2, this_run2)

        repo.record(doc.serialize())
        repo.logout()

        return doc
PropertyMean.execute()
#eof