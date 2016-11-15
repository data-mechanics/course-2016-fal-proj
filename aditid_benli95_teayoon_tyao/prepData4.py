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
    writes = ['aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment', 'aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment', 'aditid_benli95_teayoon_tyao.averageAll', 'aditid_benli95_teayoon_tyao.averageDrug']


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
            for j in range(0,int(y[i]/21.5)):           #253075 / 11800 = 21.5
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
        #plt.show()

        repo.dropPermanent('aditid_benli95_teayoon_tyao.a')
        repo.createPermanent('aditid_benli95_teayoon_tyao.a')

        repo.dropPermanent('aditid_benli95_teayoon_tyao.b')
        repo.createPermanent('aditid_benli95_teayoon_tyao.b')
        
        repo.aditid_benli95_teayoon_tyao.b = repo.aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment.find()
        repo.aditid_benli95_teayoon_tyao.a = repo.aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment.find()

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

        #reset resulting directory
        repo.dropPermanent('aditid_benli95_teayoon_tyao.averageDrug')
        repo.createPermanent('aditid_benli95_teayoon_tyao.averageDrug')


        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')
        pass

prepData4.execute()



