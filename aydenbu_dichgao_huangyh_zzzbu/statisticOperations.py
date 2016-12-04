import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
from bson.code import Code
from bson.json_util import dumps
from helpers import *


class correlation(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.public_earning_crime_boston']
    writes = ['aydenbu_huangyh.statistic_data']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # Connect to the Database
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        data = repo['aydenbu_huangyh.public_earning_crime_boston']

        avgs = []
        stores = []
        hospitals = []
        schools = []
        gardens = []
        crimes = []

        x = []
        y = []

        for document in data.find():
            avg = [document['_id'], document['value']['avg']]
            store = tuple((document['_id'], document['value']['numofStore']))
            hospital = tuple((document['_id'], document['value']['numofHospital']))
            school = tuple((document['_id'], document['value']['numofSchool']))
            garden = tuple((document['_id'], document['value']['numofGarden']))
            crime = tuple((document['_id'], document['value']['numofCrime']))

            # temp_avg = document['value']['avg']
            # temp_crime = document['value']['numofGarden']

            avg[1] = float(avg[1])
            avg = tuple(avg)

            # temp_avg = float(temp_avg)

            # x.append(temp_avg)
            # y.append(temp_crime)


            avgs.append(avg)
            stores.append(store)
            hospitals.append(hospital)
            schools.append(school)
            gardens.append(garden)
            crimes.append(crime)

        # print(avgs)
        # print(stores)
        # print(hospitals)
        # print(schools)
        # print(gardens)
        # print(crimes)


        # print(x)
        # print(y)

        '''
        Implement the statistic methods here:
        '''
        def clean(x):
            r = []
            for i in x:
                r += [i[1]]
            return r

        def cor(x, y):
            xm = sum(x) / len(x)
            ym = sum(y) / len(y)
            top = 0
            bot1 = 0
            bot2 = 0
            for i in range(len(x)):
                top += (x[i] - xm) * (y[i] - ym)
                bot1 += math.pow((x[i] - xm), 2)
                bot2 += math.pow((y[i] - ym), 2)
            bot = math.sqrt(bot1) * math.sqrt(bot2)
            return top / bot

        def least_square(x, y):
            n = len(x)
            sum_x = sum(x)
            sum_y = sum(y)
            sum_xy = sum(x[i] * y[i] for i in range(0, n))
            sum_xx = sum(math.pow(x[i], 2) for i in range(0, n))
            b = (n * sum_xy - (sum_x * sum_y)) / (n * sum_xx - math.pow(sum_x, 2))
            a = (sum_y - b * sum_x) / n
            return (a, b)

        def r_square(x, y, a, b):
            n = len(x)
            ss_res = 0
            f = [(b * x[i] + a) for i in range(0, n)]
            for i in range(0, n):
                ss_res += math.pow(y[i] - f[i], 2)
            ss_tot = 0
            for i in range(0, n):
                ss_tot += math.pow(y[i] - sum(y) / n, 2)
            r_square = 1 - (ss_res / ss_tot)
            return r_square

        '''
        Statistic methods end Here
        '''
        # print(clean(avgs))
        # print(clean(stores))
        # print(stores)
        # print(clean(hospitals))
        # print(clean(schools))
        # print(clean(gardens))
        # print(clean(crimes))

        x = clean(avgs)
        y = [clean(stores), clean(hospitals), clean(schools), clean(gardens), clean(crimes)]
        correlations =[]
        leastSquares = []
        Rsquares = []
        for i in range(len(y)):
            correlations += [cor(x, y[i])]
            leastSquares += [least_square(x, y[i])]
            Rsquares += [r_square(x, y[i], leastSquares[i][0], leastSquares[i][1])]

        index = ['stores and Avg', 'hospital and Avg', 'school and Avg', 'garden and Avg', 'crimes and Avg']
        # print('data for [stores, hospitals, schools, gardens, crimes]')
        # print('correlations: ', correlations)
        # print('leastSquares: ', leastSquares)
        # print('R square: ' , Rsquares)

        results = []
        for i in range(len(index)):
            result = {
                        '_id': index[i],
                        'Correlation': correlations[i],
                        'LeastSquare': leastSquares[i],
                        'R Square': Rsquares[i]
                      }
            results.append(result)



        repo.dropPermanent("statistic_data")
        repo.createPermanent("statistic_data")
        repo['aydenbu_huangyh.statistic_data'].insert_many(results)



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
                                           {prov.model.PROV_LABEL: "Get the correlation and leastSquare for each entry related to evg earnings"})
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



correlation.execute()
doc = correlation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
