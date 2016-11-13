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
    writes = ['']

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

            temp_avg = document['value']['avg']
            # temp_crime = document['value']['numofGarden']

            avg[1] = float(avg[1])
            avg = tuple(avg)

            temp_avg = float(temp_avg)

            # x.append(temp_avg)
            # y.append(temp_crime)


            avgs.append(avg)
            stores.append(store)
            hospitals.append(hospital)
            schools.append(school)
            gardens.append(garden)
            crimes.append(crime)

        print(avgs)
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
        print(clean(avgs))
        # print(clean(stores))
        # print(stores)
        # print(clean(hospitals))
        # print(clean(schools))
        # print(clean(gardens))
        # print(clean(crimes))

        x = clean(avgs)
        y = [clean(stores), clean(hospitals), clean(schools), clean(gardens), clean(crimes)]
        corrolations =[]
        leastSquares = []
        Rsquares = []
        for i in range(len(y)):
            corrolations += [cor(x, y[i])]
            leastSquares += [least_square(x, y[i])]
            Rsquares += [r_square(x, y[i], leastSquares[i][0], leastSquares[i][1])]

        print('data for [stores, hospitals, schools, gardens, crimes]')
        print('corrolations: ', corrolations)
        print('leastSquares: ', leastSquares)
        print('Rsquare: ' , Rsquares)






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
        return None


correlation.execute()
