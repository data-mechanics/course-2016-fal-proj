import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import sqrt
from random import shuffle

class crimes_firearms_plot(dml.Algorithm):
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

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:manda094_nwg_patels95#crimes_firearms_plot', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:manda094_nwg_patels95#firearm_recovery', {'prov:label':'Firearm Recovery', prov.model.PROV_TYPE:'ont:DataResource'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        repo.record(doc.serialize())
        repo.logout()

        return doc

crimes_firearms_plot.execute()
doc = crimes_firearms_plot.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
