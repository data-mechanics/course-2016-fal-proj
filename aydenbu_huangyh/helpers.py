import json
import dml

def getAuth(key):
    with open('../auth.json') as data_file:
        data = json.load(data_file)
        return data[key]

def openDb(u, p):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(u, p)
    return repo