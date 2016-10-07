import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import pyproj

###############################
# INPUT DATA

# Table of car accidents in Boston
data_url = 'http://datamechanics.io/data/car_accidents.xls'
team_name = 'schiao_uvarovis'
dataset_name = 'car_accidents'
collection_name = team_name + '.' + dataset_name

mass_plane = '+proj=lcc +lat_1=42.68333333333333 +lat_2=41.71666666666667 +lat_0=41 +lon_0=-71.5 +x_0=200000 +y_0=750000 +ellps=GRS80 +datum=NAD83 +units=m +no_defs'


###############################
# MAIN

class get_car_accidents(dml.Algorithm):
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

        # Cleans up the table using pandas
        df = pd.read_excel(url, skiprows=1, index_col=None)
        df = df.dropna(subset=['Manner of Collision'])
        df = df.dropna(subset=['X Coordinate'])
        df = df.dropna(subset=['Y Coordinate'])
        r = json.loads(df.to_json(orient='records'))

        new_data = list()
        mass = pyproj.Proj(mass_plane)

        for entry in r:
            # Converting coordinates to longtitude and latitude
            x, y = mass(entry['X Coordinate'], entry['Y Coordinate'], inverse=True)

            entry["location"] = {
                'type': 'Point',
                'coordinates': [x, y]
            }
            del entry['X Coordinate']
            del entry['Y Coordinate']
            new_data.append(entry)

        repo.dropPermanent(collection_name)
        repo.createPermanent(collection_name)
        repo[collection_name].ensure_index([("location", dml.pymongo.GEOSPHERE)])
        repo[collection_name].insert_many(new_data)

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
        doc.add_namespace('mcp', 'http://services.massdot.state.ma.us/crashportal/')

        this_script = doc.agent(
            'alg:' + team_name + '#get_car_accidents',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resource = doc.entity(
            'mcp:2013',
            {'prov:label':'Car Accidents', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'xls'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resource, startTime)

        car_accidents = doc.entity(
            'dat:' + team_name + '#car_accidents',
            {prov.model.PROV_LABEL:'Car Accidents',
            prov.model.PROV_TYPE:'ont:DataSet'}
        )
        doc.wasAttributedTo(car_accidents, this_script)
        doc.wasGeneratedBy(car_accidents, this_run, endTime)
        doc.wasDerivedFrom(car_accidents, resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

get_car_accidents.execute()
doc = get_car_accidents.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
