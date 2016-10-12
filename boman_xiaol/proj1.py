# boman@bu.edu xiaol.bu.edu
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pymongo

def getlist(x):
    temp=[]
    for i in repo['boman_xiaol.'+x].find({}):
        temp.append(i)
    return temp

def getcol(X, Y):
    temp = []
    for i in range(len(X)):
        temp.append(dict(zip([Y],[X[i][Y]])))
    return temp


@staticmethod
def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
startTime = datetime.datetime.now()
        
        # Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('boman_xiaol', 'boman_xiaol')

url = 'https://data.cityofnewyork.us/resource/f88x-qs6w.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("race")
repo.createPermanent("race")
repo['boman_xiaol.race'].insert_many(r)

race1 = getlist("race")
race2 = []
#map the useful columns in this dataset
for i in race1:
    race2.append({'district': i['district'],'grade': i['grade'], \
                  'year': i['year'],'race_mean_scale_score': i['mean_scale_score'],\
                  'race': i['demographic']})


url = 'https://data.cityofnewyork.us/resource/c26d-5mki.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("gender")
repo.createPermanent("gender")
repo['boman_xiaol.gender'].insert_many(r)

gender1 = getlist("gender")
#map the useful columns in this dataset
gender2 = []
for i in gender1:
    gender2.append({'district': i['district'],'grade': i['grade'], \
                  'year': i['year'],'gender_mean_scale_score': i['mean_scale_score'],\
                  'gender': i['demographic']})

        
url = 'https://data.cityofnewyork.us/resource/qm3d-sn7g.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("allmath")
repo.createPermanent("allmath")
repo['boman_xiaol.allmath'].insert_many(r)

allmath1 = getlist("allmath")
allmath2 = []
allmath3 = []
#map the useful columns in this dataset
for i in allmath1:
    allmath2.append({'district': i['district'],'grade': i['grade'], \
                  'year': i['year'],'all_mean_scale_score': i['mean_scale_score']})
for i in allmath2:
    allmath3.append(i)
#now we merge the first three data

for i in race2:
    for k in gender2:
        if i['district'] == k['district'] and i['grade'] == k['grade'] and i['year'] == k['year']:
            i.update({'gender':k['gender']})
            i.update({'gender_mean_scale_score': k['gender_mean_scale_score']})

for i in race2:
    for k in allmath2:
        if i['district'] == k['district'] and i['grade'] == k['grade'] and i['year'] == k['year']:
            i.update({'all_mean_scale_score': k['all_mean_scale_score']})

            

            


url = 'https://data.cityofnewyork.us/resource/hvnc-iy6e.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("attendance")
repo.createPermanent("attendance")
repo['boman_xiaol.attendance'].insert_many(r)

attendance1 = getlist("attendance")

'''
attendance1_dist = getcol(attendance1, "district")
attendance1_attendance = getcol(attendance1, 'ytd_attendance_avg_')
temp1=[]
for i in attendance1_dist:
    if i['district'][0:8] == 'DISTRICT':
        temp1.append(dict(zip(['district'],[i['district'][9:]])))
    else:
        temp1.append(dict(zip(['district'],['32'])))
attendance1_dist=temp1
'''



url = 'https://data.cityofnewyork.us/resource/734v-jeq5.json'
response = urllib.request.urlopen(url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)
repo.dropPermanent("sat")
repo.createPermanent("sat")
repo['boman_xiaol.sat'].insert_many(r)
sat1 = getlist('sat')
#mapping the second date
sat_att= []
for i in attendance1:
    for k in sat1:
        if k['dbn'][:2] in i['district']:
            sat_att.append({'district': i['district'][9:], 'dbn':k['dbn'], \
                            'attendance': i['ytd_attendance_avg_'], 'sat_math': k['sat_math_avg_score'],\
                            'school_name':k['school_name']})
#mapping the third data
mathavg_att=[]
for i in attendance1:
    for k in allmath3:
        if k['year']=='2010' and (k['district'] in i['district']) and k['grade'] == 'All Grades':
            mathavg_att.append({'district':k['district'], 'all_mean_scale_score': k['all_mean_scale_score'],\
                                'attendance': i['ytd_attendance_avg_']})


'''
sat1_DBN = getcol( sat1, 'dbn')
sat1_avgmath = getcol( sat1, 'sat_math_avg_score')


temp2=[]
for i in sat1_DBN:
    temp2.append(dict(zip(['district'],[i['dbn'][0:2]])))
sat1_DBN=temp2
'''
      

repo.dropPermanent("avemath")
repo.createPermanent("avemath")
repo['boman_xiaol.avemath'].insert_many(race2)

repo.dropPermanent("satatt")
repo.createPermanent("satatt")
repo['boman_xiaol.satatt'].insert_many(sat_att)

repo.dropPermanent("mathavgatt")
repo.createPermanent("mathavgatt")
repo['boman_xiaol.mathavgatt'].insert_many(mathavg_att)

repo.logout()

endTime = datetime.datetime.now()



'''




    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):


         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('boman_xiaol', 'boman_xiaol')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://nycopendata.socrata.com/resource/')

        this_script = doc.agent('alg:proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('bdp:usap-qc7e', {'prov:label':'race of shcool', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('bdp:qphc-zrtc', {'prov:label':'gender of school', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource3 = doc.entity('bdp:7yig-nj52', {'prov:label':'all math', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                }
            )
        doc.usage(get_lost, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                }
            )

        race = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/boman_xiaol/') # The scripts in <folder>/<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/boman_xiaol/') # The data sets in <user>/<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log#') # The event log.
doc.add_namespace('bdp', 'nycopendata.socrata.com/resource/')

this_script = doc.agent('alg:proj1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

resrace = doc.entity('bdp:usap-qc7e', {'prov:label':'race of shcool', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resgender = doc.entity('bdp:qphc-zrtc', {'prov:label':'gender of school', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resall = doc.entity('bdp:7yig-nj52', {'prov:label':'all math', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
resatt = doc.entity('bdp:7z8d-msnt', {'prov:label':'attendance', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
ressat = doc.entity('bdp:f9bf-2cp4', {'prov:label':'sat', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

race3 = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Race', prov.model.PROV_TYPE:'ont:Computation'})
sat_att = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'sat and attendance', prov.model.PROV_TYPE:'ont:Computation'})
mathavg_att = doc.activity('log:a'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'math avg and sat', prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(race3, this_script)
doc.wasAssociatedWith(sat_att, this_script)
doc.wasAssociatedWith(mathavg_att, this_script)

race3 = doc.entity('dat:race3', {prov.model.PROV_LABEL:'race3', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(race3, this_script)
doc.wasGeneratedBy(race3, race3, endTime)
doc.wasDerivedFrom(race3, resrace, race3, race3, race3)

sat_att = doc.entity('dat:sat_att', {prov.model.PROV_LABEL:'sat_att', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(sat_att, this_script)
doc.wasGeneratedBy(sat_att, sat_att, endTime)
doc.wasDerivedFrom(sat_att, ressat, sat_att, sat_att, sat_att)

mathavg_att = doc.entity('dat:mathavg_att', {prov.model.PROV_LABEL:'mathavg_att', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(mathavg_att, this_script)
doc.wasGeneratedBy(mathavg_att, mathavg_att, endTime)
doc.wasDerivedFrom(mathavg_att, resatt, mathavg_att, mathavg_att, mathavg_att)


print(json.dumps(json.loads(doc.serialize()), indent=4))
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
repo.logout()


