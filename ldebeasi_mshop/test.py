import dml

# Authenticate with MongoDB
contributor = 'alice_bob'
reads = []
writes = ['alice_bob.crime']

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate("alice_bob", "alice_bob")

repo.dropPermanent("streetLightCrimes")
repo.createPermanent("streetLightCrimes")

repo.dropPermanent("addressStreetLights")
repo.createPermanent("addressStreetLights")


# Get all Street Light Data
data = repo.alice_bob.streetLights.find()
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
'''data = repo.alice_bob.crime.find()

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
        res = repo.alice_bob.streetLightCrimes.insert_one(toDict)'''


# Get Master Address Data
data = repo.alice_bob.address.find()

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
    res = repo.alice_bob.addressStreetLights.insert_one(toDict)




