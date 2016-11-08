import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

###############################
# INPUT DATA

token = json.loads(open('../auth.json').read())['services']['cityofbostondataportal']['token']

# Boston street lights in json format
data_url = 'https://data.cityofboston.gov/resource/fbdp-b7et.json?$$app_token=' + token
team_name = 'schiao_uvarovis'
dataset_name = 'street_lights'
collection_name = team_name + '.' + dataset_name


###############################
# MAIN

class get_street_lights(dml.Algorithm):
    contributor = team_name
    reads = []
    writes = [collection_name]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(team_name, team_name)

        url = data_url

        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        # Get only geo points (no need for other data)
        geo_objects = list()
        for o in r:
            new_o = {
                'location': o['the_geom']
            }
            geo_objects.append(new_o)

        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent(dataset_name)
        repo.createPermanent(dataset_name)
        repo[collection_name].ensure_index([("location", dml.pymongo.GEOSPHERE)])
        repo[collection_name].insert_many(geo_objects)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client =  dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(team_name, team_name)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent(
            'alg:' + team_name + '#get_street_lights',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resource = doc.entity(
            'bdp:fbdp-b7et',
            {'prov:label':'Street Lights', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)

        street_lights = doc.entity(
            'dat:' + team_name + '#street_lights',
            {prov.model.PROV_LABEL:'Street Lights',
            prov.model.PROV_TYPE:'ont:DataSet'}
        )
        doc.wasAttributedTo(street_lights, this_script)
        doc.wasGeneratedBy(street_lights, this_run, endTime)
        doc.wasDerivedFrom(street_lights, resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

get_street_lights.execute()
doc = get_street_lights.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
