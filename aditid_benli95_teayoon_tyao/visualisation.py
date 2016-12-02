import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt


class prepData4(dml.Algorithm):

    contributor = 'aditid_benli95_teayoon_tyao'
    reads = ['aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment', 'aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment']
    writes = []


    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        all_crime = repo.aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment.find()
        x = []
        y = []
        print(all_crime)

        for crime in all_crime:
            crimeDict = dict(crime)
            x.append(crimeDict["_id"])
            y.append(crimeDict["value"]["crimes"])
        
        sumy = sum(y)
        
        all = []
        for i in range(0,len(x)):
            for j in range(0,int(y[i]/21.5)):           #253075 / 11800 = 21.5 for nomalization
                all.append(x[i])
        
        
        drug_crime = repo.aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment.find()
        a = []
        b = []
        for crime in drug_crime:
            crimeDict = dict(crime)
            a.append(crimeDict["_id"])
            b.append(crimeDict["value"]["crimes"])

        drug = []
        for i in range(0,len(a)):
            for j in range(0,int(b[i])):
                drug.append(a[i])

        bins = []
        for k in range(0,250,5):
            bins.append(k)


        plt.hist(all,bins)
        plt.hist(drug,bins,facecolor="green")

        plt.xlabel("Establisments")
        plt.ylabel("Crimes")
        plt.show()

        import statsmodels.api as sm
        model = sm.OLS(y, x)
        results2 = model.fit()
        print (results2.summary())
        print ("Confidence Intervals:", results.conf_int())
        print ("Parameters:", results2.params)

        model = sm.OLS(b, a)
        results2 = model.fit()
        print (results2.summary())
        print ("Confidence Intervals:", results.conf_int())
        print ("Parameters:", results2.params)

        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:aditid_benli95_teayoon_tyao#prepData4', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        prepD4 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Prep Data 4', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(prepD4, this_script)

        crimesPerNumberOfEstablishment = doc.entity('dat:aditid_benli95_teayoon_tyao#crimesPerNumberOfEstablishment', {'prov:label':'Number Of All Crimes per Establishments', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(prepD4, crimesPerNumberOfEstablishment, startTime)

        drugCrimesPerNumberOfEstablishment = doc.entity('dat:aditid_benli95_teayoon_tyao#drugCrimesPerNumberOfEstablishment', {'prov:label':'Number Of Drug Crimes per Establishments', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(prepD4, drugCrimesPerNumberOfEstablishment, startTime)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

prepData4.execute()
doc = prepData4.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))



