
# coding: utf-8

# In[6]:

import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from matplotlib import pyplot
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import statsmodels.api as sm


class visualisationScatter(dml.Algorithm):

    contributor = 'aditid_benli95_teayoon_tyao'
    reads = ['aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment', 'aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment']
    writes = []


    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        #Different Results from different runs
        a = [(0.0, 0.0), (0.1, -0.01453462244896253), (0.2, -0.04148314712092116), (0.3, -0.30897138920096356), (0.4, -0.5372550761230084), (0.5, -0.5208480017814772), (0.6, -0.6624578866384141), (0.7, -0.6259744465113002), (0.8, -0.7870293357800726), (0.9, -1.4708949999748882), (1.0, -1.8335521810543867), (1.1, -1.8871610211349577), (1.2, -2.1733582528124344), (1.3, -2.25644286295514), (1.4, -2.0049699208213525), (1.5, -2.528650806939055), (1.6, -2.373709977446893), (1.7, -2.828301058003504), (1.8, -2.8450637916340895), (1.9, -3.1687611698146725)]
        b = [(2.0, -3.225148001614045), (2.1, -3.1783534475108866), (2.2, -3.183822726230062), (2.3, -3.2258895500596907), (2.4, -2.7502100352109835), (2.5, -2.714177623703023), (2.6, -2.187649295027086), (2.7, -2.1326878044125124), (2.8, -1.7920159160964033), (2.9, -1.1289064004272689)]
        c = [(3.0, -1.2450464372958265), (3.1, -0.8300990277217295), (3.2, 0.016431812435854454), (3.3, 0.5118593252151129), (3.4, 0.7582651187009901), (3.5, 0.9167192598830525), (3.6, 1.3215516603405035), (3.7, 1.6277809134091399), (3.8, 1.2155871341817601), (3.9, 1.181746768978826)]
        d = [(4.0, 1.273968015443927), (4.1, 1.6170964593131885), (4.2, 1.6383009910306328), (4.3, 1.4898154646994612), (4.4, 1.444868900657525), (4.5, 1.703188685942564), (4.6, 1.565050731929432), (4.7, 1.835695094406617), (4.8, 1.597544306722284), (4.9, 1.924738060834784)]

        tuple_format = a + b + c + d
        
        x = []
        y = []
        
        for tpl in tuple_format:
            x.append(tpl[0])
            y.append(tpl[1])
        

        plt.scatter(x, y)
        pyplot.xlabel("Radius from Crime")
        pyplot.ylabel("Difference")
#         pyplot.legend(loc='upper left')
        pyplot.show()



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

        this_script = doc.agent('alg:aditid_benli95_teayoon_tyao#visualisationScatter', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        visSCAT = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Visualisation Scatter Plot', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(visSCAT, this_script)

        crimesPerNumberOfEstablishment = doc.entity('dat:aditid_benli95_teayoon_tyao#crimesPerNumberOfEstablishment', {'prov:label':'Number Of All Crimes per Establishments', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(visSCAT, crimesPerNumberOfEstablishment, startTime)

        drugCrimesPerNumberOfEstablishment = doc.entity('dat:aditid_benli95_teayoon_tyao#drugCrimesPerNumberOfEstablishment', {'prov:label':'Number Of Drug Crimes per Establishments', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(visSCAT, drugCrimesPerNumberOfEstablishment, startTime)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

visualisationScatter.execute()
doc = visualisationScatter.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))





# In[ ]:



