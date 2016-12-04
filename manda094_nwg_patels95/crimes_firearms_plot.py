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

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

        dates = []
        crimes = []
        firearmsRecovered = []
        count = 0
        for data in repo['manda094_nwg_patels95.firearm_recovery'].find().sort("collectiondate", 1):
            dates.append(data["collectiondate"])
            firearmsRecovered.append(data["totalgunsrecovered"])
            try:
                crimes.append(data["totalcrimes"])
            except KeyError:
                # add 0 to array if "totalcrimes" key does not exist
                crimes.append(0)

        f = open('crimes_firearms_graph_data.js', 'w')
        f.truncate()
        f.write("var crimes_firearms = {" + "\n" + "\"crimes\": [" + "\n")

        # dates and number of crimes
        for i in range(0, len(dates)):
            f.write("{ \"date\":\"" + str(dates[i]) + "\", \"count\":" + str(crimes[i]) + "}")
            if (i == (len(dates) - 1)):
                f.write("\n")
            else:
                f.write("," + "\n")

        f.write("], \n\"firearms\": [" + "\n")

        # dates and number of firearms recovered
        for i in range(0, len(dates)):
            f.write("{ \"date\":\"" + str(dates[i]) + "\", \"count\":" + str(firearmsRecovered[i]) + "}")
            if (i == (len(dates) - 1)):
                f.write("\n")
            else:
                f.write("," + "\n")

        f.write("] \n}")


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
# doc = firearms_crimes_analysis.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
