import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

###############################
# INPUT DATA

# Boston mbta stops in geojson format
data_url = 'http://bostonopendata.boston.opendata.arcgis.com/datasets/f1a43ad3c46b4ac89b74cdaba393ccac_4.geojson'
team_name = 'schiao_uvarovis'
dataset_name = 'mbta_stops'
collection_name = team_name + '.' + dataset_name


###############################
# MAIN

class get_mbta_stops(dml.Algorithm):
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
        features = r['features']

        # Get only geo points (no need for other data)
        geo_objects = list()
        for feature in features:
            new_o = {
                'location': feature['geometry']
            }
            geo_objects.append(new_o)

        s = json.dumps(features, sort_keys=True, indent=2)
        repo.dropPermanent(collection_name)
        repo.createPermanent(collection_name)
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
        doc.add_namespace('bmd', 'http://bostonopendata.boston.opendata.arcgis.com')

        this_script = doc.agent(
            'alg:' + team_name + '#get_mbta_stops',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resource = doc.entity(
            'bmd:f1a43ad3c46b4ac89b74cdaba393ccac_4',
            {'prov:label':'MBTA Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)

        mbta_stops = doc.entity(
            'dat:' + team_name + '#mbta_stops',
            {prov.model.PROV_LABEL:'MBTA Stops',
            prov.model.PROV_TYPE:'ont:DataSet'}
        )
        doc.wasAttributedTo(mbta_stops, this_script)
        doc.wasGeneratedBy(mbta_stops, this_run, endTime)
        doc.wasDerivedFrom(mbta_stops, resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

get_mbta_stops.execute()
doc = get_mbta_stops.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
