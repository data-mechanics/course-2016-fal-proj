import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sys
from bson.son import SON
from bson.code import Code

contributor = 'dichgao'
reads = ['pickup_hotline','pickup_311']
writes = ['control_area']

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('dichgao', 'dichgao')
startTime = datetime.datetime.now()

#basic tools
def union(R, S):
    return R + S

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]
 
reducer = Code('''
                        function(obj,prev){
                            prev.count++;
                        }
                        ''')
#count requests of picking up dead animals in every neighborhood
#results0 = repo['dichgao.request_hotline'].group(key = {"neighborhood":1},condition = {}, initial = {"count": 0},reduce = reducer)
results1 = repo['dichgao.gardens'].group(key = {"area":1},condition = {}, initial = {"count": 0},reduce = reducer)
results2 = repo['dichgao.pickup_hotline'].group(key = {"neighborhood":1},condition = {}, initial = {"count": 0},reduce = reducer)
results3 = repo['dichgao.pickup_311'].group(key = {"neighborhood":1},condition = {}, initial = {"count": 0},reduce = reducer)

#extrat data without formatting
gardens = []
for i in range(len(results1)):
    neighborhood = results1[i]['area']
    count = results1[i]['count']
    gardens.append((neighborhood,count))
    i = i+1

pickup2 = []
for i in range(len(results2)):
    neighborhood = results2[i]['neighborhood']
    deadcount = results2[i]['count']
    pickup2.append((neighborhood,deadcount))
    i = i+1

pickup3 = []
for i in range(len(results3)):
    neighborhood = results3[i]['neighborhood']
    deadcount = results3[i]['count']
    pickup3.append((neighborhood,deadcount))
    i = i+1

#find total requests in every neighborhood by combining two data sets
pickup_total = aggregate(project(union(pickup2,pickup3), lambda t:(t[0], t[1])),sum)

#merge total number of requests with number of gardens in every neighborhood
deadcount_with_gardens = project(select(product(gardens,pickup_total), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))
#for doc in deadcount_with_gardens: print(doc)

#formatting data

control_area = []
for i in range(len(deadcount_with_gardens)):
    control_area.append({'neighborhood': deadcount_with_gardens[i][0], 'gardens':deadcount_with_gardens[i][1], 'dead_animal': deadcount_with_gardens[i][2]} )
    
repo.dropPermanent("control_area")
repo.createPermanent("control_area")
repo['dichgao.control_area'].insert_many(control_area)


#provenance
startTime = None
endTime = None
doc = prov.model.ProvDocument()

doc.add_namespace('alg', 'http://datamechanics.io/algorithm/dichgao') # The scripts are in <folder>#<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/dichgao') # The data sets are in <user>#<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:retrieve_datasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
resource1 = doc.entity('dat:pickup_hotline', {prov.model.PROV_LABEL:'pickup_hotline', prov.model.PROV_TYPE:'ont:DataSet'})
resource2 = doc.entity('dat:pickup_311', {prov.model.PROV_LABEL:'pickup_311', prov.model.PROV_TYPE:'ont:DataSet'})

get_control_area = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
doc.wasAssociatedWith(get_control_area, this_script)
doc.usage(get_control_area, resource1, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )
doc.usage(get_control_area, resource2, startTime, None,
        {prov.model.PROV_TYPE:'ont:Computation'
        }
    )

control_area = doc.entity('dat:dichgao#control_area', {prov.model.PROV_LABEL:'Control Area', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(control_area, this_script)
doc.wasGeneratedBy(control_area, get_control_area, endTime)
doc.wasDerivedFrom(control_area, resource1, get_control_area, get_control_area, get_control_area)
doc.wasDerivedFrom(control_area, resource2, get_control_area, get_control_area, get_control_area)

repo.record(doc.serialize()) # Record the provenance document.

print(doc.get_provn())

repo.logout()
