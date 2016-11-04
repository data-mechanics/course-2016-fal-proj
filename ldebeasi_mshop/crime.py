import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class crime(dml.Algorithm):
    contributor = 'ldebeasi_mshop'
    reads = []
    writes = ['ldebeasi_mshop.crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston crime incident reports'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo

        repo.authenticate("ldebeasi_mshop", "ldebeasi_mshop")

        hasMore = True
        offset = 0
        first = True

        # Page through all data so we don't make our computer melt
        while hasMore:
            url = 'https://data.cityofboston.gov/resource/fqn4-4qap.json?$$app_token=' + dml.auth['bostonData'] + '&$order=:id&$limit=10000&$offset=' + str(offset)

            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)

            # If first time loading data, only create data set after we get data
            if first:
                repo.dropPermanent("crime")
                repo.createPermanent("crime")
                first = False

            # We have reached the end of the dataset
            if r == []:
                hasMore = False
                break
            # There is more data to be had -- Keep going
            else:
                offset += 10000

            s = json.dumps(r, sort_keys=True, indent=2)

            repo['ldebeasi_mshop.crime'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ldebeasi_mshop', 'ldebeasi_mshop')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ldebeasi_mshop') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/ldebeasi_mshop') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ldebeasi_mshop#crime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:fqn4-4qap', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crimes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crimes, this_script)
        doc.usage(get_crimes, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        crimes = doc.entity('dat:ldebeasi_mshop#crime', {prov.model.PROV_LABEL:'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimes, this_script)
        doc.wasGeneratedBy(crimes, get_crimes, endTime)
        doc.wasDerivedFrom(crimes, resource, get_crimes, get_crimes, get_crimes)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

crime.execute()
doc = crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof