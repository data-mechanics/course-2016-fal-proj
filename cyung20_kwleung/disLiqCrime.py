import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

contributor = "cyung20_kwleung"
reads = ['cyung20_kwleung.BPDS', 'cyung20_kwleung.liquorAndCrime']
writes = ['cyung20_kwleung.disLiqCrime']

client = dml.pymongo.MongoClient()
repo = client.repo 
repo.authenticate("cyung20_kwleung", "cyung20_kwleung") 

startTime = datetime.datetime.now()

repo.dropPermanent("disLiqCrime")
repo.createPermanent("disLiqCrime")

data1 = repo.cyung20_kwleung.BPDS.find()

districtWithCount = []
for document in data1:
    dictionary = dict(document)
    districtWithCount.append(dictionary)

data2 = repo.cyung20_kwleung.liquorAndCrime.find()

ov100nearLiq = []

# District must have at least 100 crimes - constraint must be satisfied
def dis100Crimes(crimeDetail): 
    districtNumber, crimeLocation = crimeDetail
    for x in range(0, len(districtWithCount)):
        if districtNumber == districtWithCount[x]['district']:
            if districtWithCount[x]['num_crimes_in_district'] > 100:
                '''print(districtWithCount[x])
                print(districtNumber)'''
                ov100nearLiq.append(crimeDetail)

for document in data2:
    dictionary = dict(document)
    crimeDetail = [(dictionary['crime_district']),(dictionary['crime_location'])]
    dis100Crimes(crimeDetail)

# Adds those with districts w/ 100 or more crimes and within 25 meters of liquor store
for x in range(0, len(ov100nearLiq)):
    location = ov100nearLiq[x][1]   
    district = ov100nearLiq[x][0]
    repo['cyung20_kwleung.disLiqCrime'].insert_one({'district':district, 'location':location})


endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/cyung20_kwleung') # The scripts are in <folder>#<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:disLiqCrime', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
BPDS = doc.entity('dat:cyung20_kwleung#BPDS', {prov.model.PROV_LABEL:'Boston Police District Stations', prov.model.PROV_TYPE:'ont:DataSet'})
liquorAndCrime = doc.entity('dat:cyung20_kwleung#liquorAndCrime', {prov.model.PROV_LABEL:'Liquor and Crime', prov.model.PROV_TYPE:'ont:DataSet'})

this_final = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(this_final, this_script)
doc.used(this_final, liquorLicense, startTime)
doc.used(this_final, crimeReports, startTime)

crimeLiq = doc.entity('dat:disLiqCrime', {prov.model.PROV_LABEL:'Crimes Near Liquor Stores In More Crime-Frequent Districts', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(crimeLiq, this_script)
doc.wasGeneratedBy(crimeLiq, this_final, endTime)
doc.wasDerivedFrom(crimeLiq, crimeReports, this_final, this_final, this_final)
doc.wasDerivedFrom(crimeLiq, liquorLicense, this_final, this_final, this_final)


print(repo.record(doc.serialize()))
print(doc.get_provn())

data = repo.cyung20_kwleung.disLiqCrime.find()

'''for d in data:
    print(dict(d))'''
