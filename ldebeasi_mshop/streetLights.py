import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class streetLights(dml.Algorithm):
    contributor = 'ldebeasi_mshop'
    reads = []
    writes = ['ldebeasi_mshop.streetLights']

    @staticmethod
    def execute(trial = False):
        '''Retrieve Boston Street Light Locations'''
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
            url = 'https://data.cityofboston.gov/resource/fbdp-b7et.json?$$app_token=' + dml.auth['bostonData'] + '&$order=:id&$limit=10000&$offset=' + str(offset)

            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)

            # If first time loading data, only create data set after we get data
            if first:
                repo.dropPermanent("streetLights")
                repo.createPermanent("streetLights")
                first = False

            # We have reached the end of the dataset
            if r == []:
                hasMore = False
                break
            # There is more data to be had -- Keep going
            else:
                offset += 10000

            s = json.dumps(r, sort_keys=True, indent=2)

            repo['ldebeasi_mshop.streetLights'].insert_many(r)

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

        this_script = doc.agent('alg:ldebeasi_mshop#streetLights', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:fbdp-b7et', {'prov:label':'Streetlight Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_streetlights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_streetlights, this_script)
        doc.usage(get_streetlights, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        street_lights = doc.entity('dat:ldebeasi_mshop#streetLights', {prov.model.PROV_LABEL:'Streetlight Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(street_lights, this_script)
        doc.wasGeneratedBy(street_lights, get_streetlights, endTime)
        doc.wasDerivedFrom(street_lights, resource, get_streetlights, get_streetlights, get_streetlights)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

streetLights.execute()
doc = streetLights.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof