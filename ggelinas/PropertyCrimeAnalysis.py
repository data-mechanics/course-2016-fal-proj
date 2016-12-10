import urllib.request
import json
import dml
import prov.model
import uuid
import datetime
from math import sqrt
from random import shuffle

import numpy
import matplotlib.pyplot as plt
import matplotlib.ticker as tkr
import scipy.stats

class PropertyCrimeAnalysis(dml.Algorithm):
    contributor = 'ggelinas'
    reads = ['ggelinas.stations',
             'ggelinas.districtvalue']
    writes = []


    def permute(x):
        shuffled = [xi for xi in x]
        shuffle(shuffled)
        return shuffled

    def avg(x):
        return sum(x)/len(x)

    def stddev(x):
        m = PropertyCrimeAnalysis.avg(x)
        return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

    def cov(x, y):
        return sum([(xi - PropertyCrimeAnalysis.avg(x))*(yi - PropertyCrimeAnalysis.avg(y)) for (xi,yi) in zip(x,y)])/len(x)

    def corr(x, y):
        if PropertyCrimeAnalysis.stddev(x)*PropertyCrimeAnalysis.stddev(y) != 0:
            return PropertyCrimeAnalysis.cov(x, y)/(PropertyCrimeAnalysis.stddev(x)*PropertyCrimeAnalysis.stddev(y))

    def p(x, y):
        c0 = PropertyCrimeAnalysis.corr(x, y)
        corrs = []
        for k in range(0, 2000):
            y_permuted = PropertyCrimeAnalysis.permute(y)
            corrs.append(PropertyCrimeAnalysis.corr(x, y_permuted))
        return len([c for c in corrs if abs(c) > c0])/len(corrs)

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ggelinas', 'ggelinas')

        NumCrimes = []
        PropValue = []
        count = 0

        for crime in repo['ggelinas.stations'].find().sort("location_zip", 1):
            if trial:
                count += 1
                if count <= 200:
                    try:
                        NumCrimes.append(crime['num_crimes'])
                    except KeyError:
                        NumCrimes.append(0)
            else:
                try:
                    NumCrimes.append(crime['num_crimes'])
                except KeyError:
                    NumCrimes.append(0)

        count = 0
        for value in repo['ggelinas.districtvalue'].find().sort("zip_code", 1):
            if trial:
                count += 1
                if count <= 200:
                    try:
                        PropValue.append(value['avg_value'])
                    except KeyError:
                        PropValue.append(0)
            else:
                try:
                    PropValue.append(value['avg_value'])
                except KeyError:
                    PropValue.append(0)

        print("Correlation Coefficient: " + str(PropertyCrimeAnalysis.corr(NumCrimes, PropValue)))

        print("P-value: " + str(PropertyCrimeAnalysis.p(NumCrimes, PropValue)))
        print(len(NumCrimes))
        print(PropValue)
        ############################
        districts = ['A1', 'D4', 'E13', 'B3', 'E18', 'D14', 'A7', 'C6', 'B2', 'E5', 'C11']
        scat = plt.scatter(NumCrimes, PropValue, alpha=0.5)
        plt.plot(NumCrimes, numpy.poly1d(numpy.polyfit(NumCrimes, PropValue, 1))(NumCrimes))
        for i, txt in enumerate(districts):
            plt.annotate(txt, (NumCrimes[i], PropValue[i]))
        plt.xlabel('Number of Crimes in Police District')
        plt.ylabel('Average Property Value')
        plt.title("Crime Rates and Average Property Value within Police Districts")
        plt.ylim(0, 12000000)
        plt.xlim(0, 180)

        plt.show()

        ####################################
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

        districtvalue_resource = doc.entity('dat:ggelinas#districtvalue', {'prov:label': 'Property Value in District',
                                                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                                                   'ont:Extension': 'json'})
        this_run2 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                 {'prov:label': 'Get Crime Incidents District Report Data'})
        doc.wasAssociatedWith(this_run2, this_script)
        doc.usage(
            this_run2,
            districtvalue_resource,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        stations = doc.entity('dat:ggelinas#stations',
                              {prov.model.PROV_LABEL: 'Districts incident count', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, this_run, endTime)
        doc.wasDerivedFrom(stations, stations_resource, this_run, this_run, this_run)

        incidents = doc.entity('dat:ggelinas#districtvalue',
                               {prov.model.PROV_LABEL: 'Counted incidents', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(incidents, this_script)
        doc.wasGeneratedBy(incidents, this_run2, endTime)
        doc.wasDerivedFrom(incidents, districtvalue_resource, this_run2, this_run2, this_run2)

        repo.record(doc.serialize())
        repo.logout()

        return doc


PropertyCrimeAnalysis.execute()

#eof