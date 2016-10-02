import dml

# Authenticate with MongoDB
contributor = 'alice_bob'
reads = []
writes = ['alice_bob.crime']

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate("alice_bob", "alice_bob")

repo.dropPermanent("maintenance")
repo.createPermanent("maintenance")


# Get all 311 Request Data
data = repo.alice_bob.threeOneOne.find()

# For each 311 request location
for document in data:
    toDict = dict(document)
    isClosed = toDict['case_status']

    if isClosed == 'Closed':
        repo.alice_bob.maintenance.insert_one(toDict)


data = repo.alice_bob.permits.find()

for document in data:
    toDict = dict(document)

    repo.alice_bob.maintenance.insert_one(toDict)

