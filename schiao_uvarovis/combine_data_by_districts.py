import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd

###############################
# INPUT DATA

team_name = 'schiao_uvarovis'

lights_collection     = team_name + '.street_lights'
signals_collection    = team_name + '.traffic_signals'
accidents_collection  = team_name + '.car_accidents'
mbta_collection       = team_name + '.mbta_stops'
districts_collection  = team_name + '.transportation_districts'
analysis_collection   = team_name + '.districts_overview'

###############################
# MAIN

class combine_data_by_districts(dml.Algorithm):
    contributor = team_name
    reads = [lights_collection,
             signals_collection,
             accidents_collection,
             mbta_collection,
             districts_collection]
    writes = [analysis_collection]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(team_name, team_name)

        districts_overview = list()
        for doc in repo[districts_collection].find():
            num_of_lights = repo[lights_collection].find({
                "location": {
                    '$geoWithin': {
                        '$geometry': doc
                    }
                }
            }).count()

            num_of_signals = repo[signals_collection].find({
                "location": {
                    '$geoWithin': {
                        '$geometry': doc
                    }
                }
            }).count()

            num_of_accidents = repo[accidents_collection].find({
                "location": {
                    '$geoWithin': {
                        '$geometry': doc
                    }
                }
            }).count()

            num_of_mbta = repo[mbta_collection].find({
                "location": {
                    '$geoWithin': {
                        '$geometry': doc
                    }
                }
            }).count()

            doc['numOfLights'] = num_of_lights
            doc['numOfTrafficSignals'] = num_of_signals
            doc['numOfAccidents'] = num_of_accidents
            doc['numOfMbtaStops'] = num_of_mbta
            districts_overview.append(doc)

        repo.dropPermanent(analysis_collection)
        repo.createPermanent(analysis_collection)
        repo[analysis_collection].insert_many(districts_overview)

        # Wrap up..
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

        this_script = doc.agent(
            'alg:' + team_name + '#combine_data_by_districts',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resource_lights = doc.entity(
            'dat:' + team_name + '#street_lights',
            {'prov:label':'Street Lights', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        resource_signals = doc.entity(
            'dat:' + team_name + '#traffic_signals',
            {'prov:label':'Traffic Signals', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        resource_mbta_stops = doc.entity(
            'dat:' + team_name + '#mbta_stops',
            {'prov:label':'MBTA Stops', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        resource_accidents = doc.entity(
            'dat:' + team_name + '#car_accidents',
            {'prov:label':'Car Accidents', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        resource_districts = doc.entity(
            'dat:' + team_name + '#transportation_districts',
            {'prov:label':'Transportation Districts', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource_lights, startTime)
        doc.used(this_run, resource_signals, startTime)
        doc.used(this_run, resource_mbta_stops, startTime)
        doc.used(this_run, resource_accidents, startTime)

        districts_overview = doc.entity(
            'dat:' + team_name + '#districts_overview',
            {prov.model.PROV_LABEL:'Districts Overview', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        doc.wasAttributedTo(districts_overview, this_script)
        doc.wasGeneratedBy(districts_overview, this_run, endTime)
        doc.wasDerivedFrom(districts_overview, resource_lights, this_run, this_run, this_run)
        doc.wasDerivedFrom(districts_overview, resource_signals, this_run, this_run, this_run)
        doc.wasDerivedFrom(districts_overview, resource_mbta_stops, this_run, this_run, this_run)
        doc.wasDerivedFrom(districts_overview, resource_accidents, this_run, this_run, this_run)
        doc.wasDerivedFrom(districts_overview, resource_districts, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

combine_data_by_districts.execute()
doc = combine_data_by_districts.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
