import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from random import shuffle
from math import sqrt


class p_value(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = ['jzhou94_katerin.crime', 'jzhou94_katerin.avg_earnings']
    writes = []
        
    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        avg_earnings = [doc for doc in repo.jzhou94_katerin.avg_earnings.find()]
        crimes = [doc for doc in repo.jzhou94_katerin.crime.find()]

        # tuple (zipcode, avg_earning)
        zip_avg = [(item['_id'], float(item['value']['avg'])) for item in avg_earnings]
        # tuple (zipcode, number_crimes)
        zip_crimes = [(item['_id'], item['value']['crime']) for item in crimes]

        data = [(x2, y2) for (x1, x2) in zip_avg for (y1, y2) in zip_crimes if x1 == y1]
        
        x = [xi for (xi, yi) in data]
        y = [yi for (xi, yi) in data]
        
        def permute(x):
            shuffled = [xi for xi in x]
            shuffle(shuffled)
            return shuffled

        def avg(x): # Average
            return sum(x)/len(x)

        def stddev(x): # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

        def cov(x, y): # Covariance.
            return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

        def corr(x, y): # Correlation coefficient.
            if stddev(x)*stddev(y) != 0:
                return cov(x, y)/(stddev(x)*stddev(y))

        def p(x, y):
            c0 = corr(x, y)
            corrs = []
            for k in range(0, 2000):
                y_permuted = permute(y)
                corrs.append(corr(x, y_permuted))
            return len([c for c in corrs if abs(c) > c0])/len(corrs)

        print('p_value of the average employee earning and the number of crime is equal to', p(x, y))

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}


p_value.execute()
print("p_value Algorithm Done")

## eof
