import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import sqrt
import matplotlib.pyplot
from random import shuffle

class firearms_crimes_analysis(dml.Algorithm):
    contributor = 'manda094_nwg_patels95'
    reads = ['manda094_nwg_patels95.firearm_recovery']
    writes = []

    def permute(x):
        shuffled = [xi for xi in x]
        shuffle(shuffled)
        return shuffled

    def avg(x): # Average
        return sum(x)/len(x)

    def stddev(x): # Standard deviation.
        m = firearms_crimes_analysis.avg(x)
        return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

    def cov(x, y): # Covariance.
        return sum([(xi - firearms_crimes_analysis.avg(x))*(yi - firearms_crimes_analysis.avg(y)) for (xi,yi) in zip(x,y)])/len(x)

    def corr(x, y): # Correlation coefficient.
        if firearms_crimes_analysis.stddev(x) * firearms_crimes_analysis.stddev(y) != 0:
            return firearms_crimes_analysis.cov(x, y) / (firearms_crimes_analysis.stddev(x) * firearms_crimes_analysis.stddev(y))

    def p(x, y):
        c0 = firearms_crimes_analysis.corr(x, y)
        corrs = []
        for k in range(0, 2000):
            y_permuted = firearms_crimes_analysis.permute(y)
            corrs.append(firearms_crimes_analysis.corr(x, y_permuted))
        return len([c for c in corrs if abs(c) > c0])/len(corrs)

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

        crimes = []
        dates = []
        firearmsRecovered = []
        for data in repo['manda094_nwg_patels95.firearm_recovery'].find():
            dates.append(data["collectiondate"])
            try:
                crimes.append(data["totalcrimes"])
            except KeyError:
                # add 0 to array if "totalcrimes" key does not exist
                crimes.append(0)
            firearmsRecovered.append(data["totalgunsrecovered"])

        # print(dates)

        print("Crimes")
        print(crimes)
        print("Guns Recovered")
        print(firearmsRecovered)

        # average crimes and firearms recovered per day
        # print(firearms_crimes_analysis.avg(crimes))
        # print(firearms_crimes_analysis.avg(firearmsRecovered))

        days = list(range(1, 240))

        # correlation coefficient
        print(firearms_crimes_analysis.corr(firearmsRecovered, crimes))

        # p-value
        # print(firearms_crimes_analysis.p(firearmsRecovered, crimes))

        matplotlib.pyplot.scatter(days, crimes)
        matplotlib.pyplot.show()

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

        # repo.record(doc.serialize())
        repo.logout()

        return doc

firearms_crimes_analysis.execute()
# doc = crimes.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
