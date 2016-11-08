import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class firearms_crimes_analysis(dml.Algorithm):
    contributor = 'manda094_nwg_patels95'
    reads = ['manda094_nwg_patels95.firearm_recovery']
    writes = []

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

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

        crimes = []
        firearmsRecovered = []
        for data in repo['manda094_nwg_patels95.firearm_recovery'].find():
            try:
                crimes.append(data["totalcrimes"])
            except KeyError:
                # add 0 to array if "totalcrimes" key does not exist
                crimes.append(0)
            firearmsRecovered.append(data["totalgunsrecovered"])

        print("Crimes")
        print(crimes)
        print("Guns Recovered")
        print(firearmsRecovered)

        print(firearms_crimes_analysis.avg(crimes))
        print(firearms_crimes_analysis.avg(firearmsRecovered))

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
