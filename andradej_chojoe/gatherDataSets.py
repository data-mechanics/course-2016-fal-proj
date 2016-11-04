
# coding: utf-8

# In[1]:

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class gatherDataSets(dml.Algorithm):
    contributor = 'andradej_chojoe'
    reads = []
    writes = ['andrade_chojoe.bigbelly', 'andrade_chojoe.trashSch', 'andrade_chojoe.codeEnf', 'andrade_chojoe.foodEst', 'andrade_chojoe.hotline']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        #Set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('andradej_chojoe', 'andradej_chojoe')
        
        dataSets = {'bigbelly': 'https://data.cityofboston.gov/resource/nybq-xu5r.json',                    'trashSch': 'https://data.cityofboston.gov/resource/je5q-tbjf.json',                    'codeEnf': 'https://data.cityofboston.gov/resource/w39n-pvs8.json',                    'foodEst': 'https://data.cityofboston.gov/resource/427a-3cn5.json',                    'hotline' : 'https://data.cityofboston.gov/resource/jbcd-dknd.json'}
        
        for ds in dataSets:
            url = dataSets[ds]
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            repo.dropPermanent(ds)
            repo.createPermanent(ds)
            repo['andradej_chojoe.' + ds].insert_many(r)
        
        repo.logout()
        
        endTime = datetime.datetime.now()
        
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        #set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('andradej_chojoe', 'andradej_chojoe')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/andradej_chojoe') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/andradej_chojoe') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
    
        this_script = doc.agent('alg:andradej_chojoe#gatherDataSets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        bigbelly_rsc = doc.entity('bdp:nybq-xu5r', {'prov:label':'Big Belly Reports 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bigbelly = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Big Belly Reports 2014'})
        doc.wasAssociatedWith(get_bigbelly, this_script)
        doc.usage(
            get_bigbelly,
            bigbelly_rsc,
            startTime,
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        
        trashSch_rsc = doc.entity('bdp:je5q-tbjf', {'prov:label':'Trash Schedules by Address', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_trashSch = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Trash Schedules by Address '})
        doc.wasAssociatedWith(get_trashSch, this_script)
        doc.usage(
            get_trashSch,
            trashSch_rsc,
            startTime,
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        
        codeEnf_rsc = doc.entity('bdp:w39n-pvs8', {'prov:label':'Code Enforcement - Building and Property Violations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_codeEnf = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Code Enforcement - Building and Property Violations'})
        doc.wasAssociatedWith(get_codeEnf, this_script)
        doc.usage(
            get_codeEnf,
            codeEnf_rsc,
            startTime,
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        
        foodEst_rsc = doc.entity('bdp:427a-3cn5', {'prov:label':'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_foodEst = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Food Establishment Inspections'})
        doc.wasAssociatedWith(get_foodEst, this_script)
        doc.usage(
            get_foodEst,
            foodEst_rsc,
            startTime,
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        
        hotline_rsc = doc.entity('bdp:jbcd-dknd', {'prov:label':'Mayors 24 Hour Hotline', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_hotline = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Mayors 24 Hour Hotline'})
        doc.wasAssociatedWith(get_hotline, this_script)
        doc.usage(
            get_hotline,
            hotline_rsc,
            startTime,
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        
        bigbelly = doc.entity('dat:andradej_chojoe#bigbelly', {prov.model.PROV_LABEL:'Big Belly Reports 2014', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bigbelly, this_script)
        doc.wasGeneratedBy(bigbelly, get_bigbelly, endTime)
        doc.wasDerivedFrom(bigbelly, bigbelly_rsc, get_bigbelly, get_bigbelly, get_bigbelly)
        
        trashSch = doc.entity('dat:andradej_chojoe#trashSch', {prov.model.PROV_LABEL:'Trash Schedules by Address', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(trashSch, this_script)
        doc.wasGeneratedBy(trashSch, get_trashSch, endTime)
        doc.wasDerivedFrom(trashSch, trashSch_rsc, get_trashSch, get_trashSch, get_trashSch)
        
        codeEnf = doc.entity('dat:andradej_chojoe#codeEnf', {prov.model.PROV_LABEL:'Code Enforcement - Building and Property Violations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(codeEnf, this_script)
        doc.wasGeneratedBy(codeEnf, get_codeEnf, endTime)
        doc.wasDerivedFrom(codeEnf, codeEnf_rsc, get_codeEnf, get_codeEnf, get_codeEnf)
        
        foodEst = doc.entity('dat:andradej_chojoe#foodEst', {prov.model.PROV_LABEL:'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(foodEst, this_script)
        doc.wasGeneratedBy(foodEst, get_foodEst, endTime)
        doc.wasDerivedFrom(foodEst, foodEst_rsc, get_foodEst, get_foodEst, get_foodEst)
        
        hotline = doc.entity('dat:andradej_chojoe#hotline', {prov.model.PROV_LABEL:'Mayors 24 Hour Hotline', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hotline, this_script)
        doc.wasGeneratedBy(hotline, get_hotline, endTime)
        doc.wasDerivedFrom(hotline, hotline_rsc, get_hotline, get_hotline, get_hotline)
        
        repo.record(doc.serialize())
        repo.logout()
        
        return doc
    
gatherDataSets.execute()
doc = gatherDataSets.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))









# In[ ]:



