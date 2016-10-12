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
analysis_collection   = team_name + '.accidents_analysis'

###############################
# MAIN

class analyze_car_accidents(dml.Algorithm):
    contributor = team_name
    reads = [lights_collection,
             signals_collection,
             accidents_collection,
             mbta_collection]
    writes = [analysis_collection]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(team_name, team_name)

        accidents_count = repo[accidents_collection].count()
        current = 0
        accidents_analysis = list()
        max_distance = 100

        for doc in repo[accidents_collection].find():
            num_of_lights = len(repo.command(
                'geoNear', lights_collection,
                near = doc['location'],
                spherical = True,
                maxDistance = max_distance)['results'])

            num_of_signals = len(repo.command(
                'geoNear', signals_collection,
                near = doc['location'],
                spherical = True,
                maxDistance = max_distance)['results'])

            num_of_mbta = len(repo.command(
                'geoNear', mbta_collection,
                near = doc['location'],
                spherical = True,
                maxDistance = max_distance)['results'])

            near = {}
            near['numOfLights'] = num_of_lights
            near['numOfTrafficSignals'] = num_of_signals
            near['numOfMbtaStops'] = num_of_mbta
            doc['near'] = near
            accidents_analysis.append(doc)
            current += 1
            print("Finished {0}/{1}. {2}%".format(current, accidents_count, round(current/accidents_count*100, 2)))

        repo.dropPermanent(analysis_collection)
        repo.createPermanent(analysis_collection)
        repo[analysis_collection].insert_many(accidents_analysis)

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
            'alg:' + team_name + '#analyze_car_accidents',
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
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource_lights, startTime)
        doc.used(this_run, resource_signals, startTime)
        doc.used(this_run, resource_mbta_stops, startTime)
        doc.used(this_run, resource_accidents, startTime)

        accidents_analysis = doc.entity(
            'dat:' + team_name + '#accidents_analysis',
            {prov.model.PROV_LABEL:'Accidents Analysis', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        doc.wasAttributedTo(accidents_analysis, this_script)
        doc.wasGeneratedBy(accidents_analysis, this_run, endTime)
        doc.wasDerivedFrom(accidents_analysis, resource_lights, this_run, this_run, this_run)
        doc.wasDerivedFrom(accidents_analysis, resource_signals, this_run, this_run, this_run)
        doc.wasDerivedFrom(accidents_analysis, resource_mbta_stops, this_run, this_run, this_run)
        doc.wasDerivedFrom(accidents_analysis, resource_accidents, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

analyze_car_accidents.execute()
doc = analyze_car_accidents.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
