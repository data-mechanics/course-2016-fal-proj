import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import random
import kmeans


###############################
# INPUT DATA

team_name = 'schiao_uvarovis'
accidents_collection  = team_name + '.car_accidents'
clusters_collection  = team_name + '.accident_clusters'


###############################
# MAIN

class accident_clusters(dml.Algorithm):
    contributor = team_name
    reads = [accidents_collection]
    writes = [clusters_collection]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Setup and connect to mongo
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(team_name, team_name)

        # Get all data needed
        accidents_data = [doc for doc in repo[accidents_collection].find()]

        if trial:
            # take 200 random records if in trial mode
            accidents_data = random.sample(accidents_data, 200)

        # list of all coordinate tuples
        P = [(doc['location']['coordinates'][0], doc['location']['coordinates'][1]) for doc in accidents_data]

        # Compute min and max coordinates
        minX = accidents_data[0]['location']['coordinates'][0]
        maxX = accidents_data[0]['location']['coordinates'][0]
        minY = accidents_data[0]['location']['coordinates'][1]
        maxY = accidents_data[0]['location']['coordinates'][1]

        for doc in accidents_data:
            if doc['location']['coordinates'][0] < minX:
                minX = doc['location']['coordinates'][0]
            if doc['location']['coordinates'][0] > maxX:
                maxX = doc['location']['coordinates'][0]
            if doc['location']['coordinates'][1] < minY:
                minY = doc['location']['coordinates'][1]
            if doc['location']['coordinates'][1] > maxY:
                maxY = doc['location']['coordinates'][1]

        # starting point
        M = [(minX, minY), (maxX, maxY)]

        # Run the algorithm
        clusters = kmeans.run(M,P)
        print("Final clusters:", clusters)

        # Save results to DB
        repo.dropPermanent(clusters_collection)
        repo.createPermanent(clusters_collection)
        doc = [{"loc": [x, y]} for (x, y) in clusters]
        repo[clusters_collection].insert_many(doc)

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
            'alg:' + team_name + '#accident_clusters',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
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
        doc.used(this_run, resource_accidents, startTime)

        # Writes
        accident_clusters = doc.entity(
            'dat:' + team_name + '#accident_clusters',
            {prov.model.PROV_LABEL:'Accident Clusters', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        doc.wasAttributedTo(accident_clusters, this_script)
        doc.wasGeneratedBy(accident_clusters, this_run, endTime)
        doc.wasDerivedFrom(accident_clusters, resource_accidents, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


accident_clusters.execute()
doc = accident_clusters.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
