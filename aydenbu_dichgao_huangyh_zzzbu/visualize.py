import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from helpers import *


class visualize(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.public_earning_crime_boston']
    writes = ['aydenbu_huangyh.visualizion']

    @staticmethod
    def execute(trial=False):
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        data = repo['aydenbu_huangyh.public_earning_crime_boston']

        zip_array = []

        for document in data.find():
            zip_array.append(int(document['_id']))

        print(zip_array)
        print(len(zip_array))

        y_array = []
        crime = []
        garden = []
        store = []
        school = []
        hospital = []

        for document in data.find():
            crime.append(document['value']['numofCrime'])
            garden.append(document['value']['numofGarden'])
            store.append(document['value']['numofStore'])
            school.append(document['value']['numofSchool'])
            hospital.append(document['value']['numofHospital'])

        y_array.append(crime)
        y_array.append(garden)
        y_array.append(store)
        y_array.append(school)
        y_array.append(hospital)

        print(len(crime))

        print(y_array)

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aydenbu_huangyh#correlation',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:public_earning_crime_boston',
                              {'prov:label': 'Public Earning Crime Boston', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_statistic_reaults = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                             {
                                                 prov.model.PROV_LABEL: "Get the correlation and leastSquare for each entry related to evg earnings"})
        doc.wasAssociatedWith(get_statistic_reaults, this_script)
        doc.usage(get_statistic_reaults, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        statistic_data = doc.entity('dat:aydenbu_huangyh#statistic_data',
                                    {prov.model.PROV_LABEL: 'Statistic Results',
                                     prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(statistic_data, this_script)
        doc.wasGeneratedBy(statistic_data, statistic_data, endTime)
        doc.wasDerivedFrom(statistic_data, resource, statistic_data, statistic_data,
                           statistic_data)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc









visualize.execute()
doc = visualize.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))