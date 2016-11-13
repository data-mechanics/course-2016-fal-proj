import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import sqrt
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

    def p(x, y, trial):
        c0 = firearms_crimes_analysis.corr(x, y)
        corrs = []
        num = 2000
        if trial:
            num = 500
        for k in range(0, num):
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
        firearmsRecovered = []
        count = 0
        for data in repo['manda094_nwg_patels95.firearm_recovery'].find():
            if trial:
                # if trial mode -> only use 200 data values
                count += 1
                if count <= 200:
                    try:
                        crimes.append(data["totalcrimes"])
                    except KeyError:
                        # add 0 to array if "totalcrimes" key does not exist
                        crimes.append(0)
                    firearmsRecovered.append(data["totalgunsrecovered"])
            else:
                try:
                    crimes.append(data["totalcrimes"])
                except KeyError:
                    # add 0 to array if "totalcrimes" key does not exist
                    crimes.append(0)
                firearmsRecovered.append(data["totalgunsrecovered"])

        # average crimes and firearms recovered per day
        print("Average crimes per day: " + str(firearms_crimes_analysis.avg(crimes)))
        print("Average firearms recovered per day: " + str(firearms_crimes_analysis.avg(firearmsRecovered)))

        days = list(range(1, 240))

        # correlation coefficient
        print("Correlation Coefficient: " + str(firearms_crimes_analysis.corr(firearmsRecovered, crimes)))

        # p-value
        print("P-value: " + str(firearms_crimes_analysis.p(firearmsRecovered, crimes, trial)))
        # p-value is too high to reject the null hypothesis

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:manda094_nwg_patels95#firearms_crimes_analysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:manda094_nwg_patels95#firearm_recovery', {'prov:label':'Firearm Recovery', prov.model.PROV_TYPE:'ont:DataResource'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        repo.record(doc.serialize())
        repo.logout()

        return doc

firearms_crimes_analysis.execute()
doc = firearms_crimes_analysis.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
