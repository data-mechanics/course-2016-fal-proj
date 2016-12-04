import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from random import shuffle
from math import sqrt
import pval


###############################
# INPUT DATA

team_name = 'schiao_uvarovis'
districts_collection  = team_name + '.districts_overview'
correlation_collection  = team_name + '.la_correlation'


###############################
# MAIN

class lights_accidents_correlation(dml.Algorithm):
    contributor = team_name
    reads = [districts_collection]
    writes = [correlation_collection]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Setup and connect to mongo
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(team_name, team_name)

        # Get all data needed - tuples (district, street lights), (district, car accidents)
        districts_data = [doc for doc in repo[districts_collection].find()]

        # (number of lights, number of accidents)
        data = [(doc["numOfLights"], doc["numOfAccidents"]) for doc in districts_data]

        # right now it wouldn't change anything
        # since there are only 6 transportation districts
        if trial:
            data = random.sample(data, 100)

        # Run the algorithm
        correlation = pval.run(data)

        # Save results to DB
        repo.dropPermanent(correlation_collection)
        repo.createPermanent(correlation_collection)
        repo[correlation_collection].insert_one({"pval": correlation})

        # Wrap up..
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client =  dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(team_name, team_name)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        # Reads
        this_script = doc.agent(
            'alg:' + team_name + '#lights_accidents_correlation',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resource_districts = doc.entity(
            'dat:' + team_name + '#districts_overview',
            {'prov:label':'Car Accidents', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource_districts, startTime)

        # Writes
        la_correlation = doc.entity(
            'dat:' + team_name + '#la_correlation',
            {prov.model.PROV_LABEL:'Lights Accidents Correlation', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        doc.wasAttributedTo(la_correlation, this_script)
        doc.wasGeneratedBy(la_correlation, this_run, endTime)
        doc.wasDerivedFrom(la_correlation, resource_districts, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


lights_accidents_correlation.execute()
doc = lights_accidents_correlation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
