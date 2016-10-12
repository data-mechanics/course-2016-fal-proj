import dml
import prov.model
import uuid
import datetime

# Authenticate with MongoDB
contributor = 'ldebeasi_mshop'
reads = []
writes = ['ldebeasi_mshop.streetLightCrimes']

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate("ldebeasi_mshop", "ldebeasi_mshop")

startTime = datetime.datetime.now()

repo.dropPermanent("streetLightCrimes")
repo.createPermanent("streetLightCrimes")

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


# Get all crime data
data = repo.ldebeasi_mshop.crime.find()

# for each crime incident
for document in data:
    toDict = dict(document)

    # some incidents do not have locations, so we just ignore them
    if 'lat' in toDict and 'long' in toDict:
        lat = round(float(toDict['lat']), 3)
        long = round(float(toDict['long']), 3)
        key = (lat,long)


        id = toDict['_id']
        num = 0
        # Find if there are street lights for a location
        if key in streetLightCounts:
            num = streetLightCounts[key]

        toDict['streetLightCount'] = num

        # Create a new entry
        res = repo.ldebeasi_mshop.streetLightCrimes.insert_one(toDict)


endTime = datetime.datetime.now()

# Provenance Data
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ldebeasi_mshop') # The scripts are in <folder>#<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/ldebeasi_mshop') # The data sets are in <user>#<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:streetLightCrimes', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
streetLights = doc.entity('dat:ldebeasi_mshop#streetLights', {prov.model.PROV_LABEL:'Streetlight Locations', prov.model.PROV_TYPE:'ont:DataSet'})

crime = doc.entity('dat:ldebeasi_mshop#crime', {prov.model.PROV_LABEL:'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})

this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, streetLights, startTime)
doc.used(this_run, crime, startTime)

# Our new combined data set
streetLightCrimes = doc.entity('dat:streetLightCrimes', {prov.model.PROV_LABEL:'Street Light Count Around Crime Location', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(streetLightCrimes, this_script)
doc.wasGeneratedBy(streetLightCrimes, this_run, endTime)
doc.wasDerivedFrom(streetLightCrimes, streetLights, this_run, this_run, this_run)
doc.wasDerivedFrom(streetLightCrimes, crime, this_run, this_run, this_run)

#print(repo.record(doc.serialize()))

#print(doc.get_provn())
