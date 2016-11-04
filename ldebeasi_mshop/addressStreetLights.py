import dml
import prov.model
import uuid
import datetime

# Authenticate with MongoDB
contributor = 'ldebeasi_mshop'
reads = []
writes = ['ldebeasi_mshop.addressStreetLights']

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate("ldebeasi_mshop", "ldebeasi_mshop")

startTime = datetime.datetime.now()

repo.dropPermanent("addressStreetLights")
repo.createPermanent("addressStreetLights")

# Get all Street Light Data
data = repo.ldebeasi_mshop.streetLights.find()
streetLightCounts = {}

# For each street light location
for document in data:
    toDict = dict(document)
    # Round lat/long to 3 decimal points
    # "The third decimal place is worth up to 110 m: it can identify a large agricultural field or institutional campus."
    # Source: http://gis.stackexchange.com/questions/8650/measuring-accuracy-of-latitude-and-longitude
    lat = round(float(toDict['lat']), 3)
    long = round(float(toDict['long']), 3)

    # Get counts of street lights in a given area
    key = (lat,long)
    if key not in streetLightCounts:
        streetLightCounts[key] = 1
    else:
        streetLightCounts[key] += 1

# Get Master Address Data
data = repo.ldebeasi_mshop.address.find()

# for each address
for document in data:
    toDict = dict(document)
    lat = round(float(toDict['latitude']), 3)
    long = round(float(toDict['longitude']), 3)
    key = (lat,long)
    id = toDict['_id']

    num = 0

    # Find if there are street lights for a location
    if key in streetLightCounts:
        num = streetLightCounts[key]

    toDict['streetLightCount'] = num

    # Create a new entry
    res = repo.ldebeasi_mshop.addressStreetLights.insert_one(toDict)

endTime = datetime.datetime.now()


# Provenance Data
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ldebeasi_mshop') # The scripts are in <folder>#<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ldebeasi_mshop') # The data sets are in <user>#<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:addressStreetLights', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
masterAddress = doc.entity('dat:ldebeasi_mshop#masterAddress', {prov.model.PROV_LABEL:'Master Address List', prov.model.PROV_TYPE:'ont:DataSet'})
streetLights = doc.entity('dat:ldebeasi_mshop#streetLights', {prov.model.PROV_LABEL:'Street Light Locations', prov.model.PROV_TYPE:'ont:DataSet'})

this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, masterAddress, startTime)
doc.used(this_run, streetLights, startTime)

# Our new combined data set
maintenance = doc.entity('dat:addressStreetLights', {prov.model.PROV_LABEL:'Number of street lights around addresses', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(maintenance, this_script)
doc.wasGeneratedBy(maintenance, this_run, endTime)
doc.wasDerivedFrom(maintenance, masterAddress, this_run, this_run, this_run)
doc.wasDerivedFrom(maintenance, streetLights, this_run, this_run, this_run)

#print(repo.record(doc.serialize()))

#print(doc.get_provn())