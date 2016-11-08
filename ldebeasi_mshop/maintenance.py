import dml
import prov.model
import uuid
import datetime

# Authenticate with MongoDB
contributor = 'ldebeasi_mshop'
reads = []
writes = ['ldebeasi_mshop.maintenance']

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate("ldebeasi_mshop", "ldebeasi_mshop")

startTime = datetime.datetime.now()

repo.dropPermanent("maintenance")
repo.createPermanent("maintenance")


# Get all 311 Request Data
data = repo.ldebeasi_mshop.threeOneOne.find()

# For each 311 request location
for document in data:
    toDict = dict(document)
    isClosed = toDict['case_status']

    # only insert if service request is closed
    if isClosed == 'Closed':
        repo.ldebeasi_mshop.maintenance.insert_one(toDict)


data = repo.ldebeasi_mshop.permits.find()

# Get all approved building permits and insert into new collection
for document in data:
    toDict = dict(document)

    repo.ldebeasi_mshop.maintenance.insert_one(toDict)


endTime = datetime.datetime.now()

# Provenance Data
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ldebeasi_mshop') # The scripts are in <folder>#<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ldebeasi_mshop') # The data sets are in <user>#<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:maintenance', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
threeOneOne = doc.entity('dat:ldebeasi_mshop#threeOneOne', {prov.model.PROV_LABEL:'311, Service Requests', prov.model.PROV_TYPE:'ont:DataSet'})
approvedBuildingPermits = doc.entity('dat:ldebeasi_mshop#approvedBuildingPermits', {prov.model.PROV_LABEL:'Approved Building Permits', prov.model.PROV_TYPE:'ont:DataSet'})

this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, threeOneOne, startTime)
doc.used(this_run, approvedBuildingPermits, startTime)

# Our new combined data set
maintenance = doc.entity('dat:maintenance', {prov.model.PROV_LABEL:'Closed 311 Requests and Approved Building Permits', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(maintenance, this_script)
doc.wasGeneratedBy(maintenance, this_run, endTime)
doc.wasDerivedFrom(maintenance, threeOneOne, this_run, this_run, this_run)
doc.wasDerivedFrom(maintenance, approvedBuildingPermits, this_run, this_run, this_run)

#print(repo.record(doc.serialize()))

#print(doc.get_provn())