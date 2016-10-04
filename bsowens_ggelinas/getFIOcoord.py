import urllib.request
import urllib.parse
import json
import dml
import prov.model
import datetime
import uuid


class getFIOcoord(dml.Algorithm):
    contributor = 'bsowens_ggelinas'
    reads = ['bsowens_ggelinas.fio']
    writes = ['bsowens_ggelinas.fio']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bsowens_ggelinas', 'bsowens_ggelinas')




        def coord_query(item):
            #returns a tuple of (lat,long)

            address_str = item["location"]
            google_key = AIzaSyA8VYW_KUzsrG_1d1ow7_fql6wxRNvq5O8
            url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + address_str + "+MA&key=" + google_key
            url = url.replace(" ", "+")
            try:
                urlres = urllib.request.urlopen(url).read().decode("utf-8")
                result = json.loads(urlres)
                return result['results'][0]['geometry']['location']
            except:
                #print("Google Geocoding error: probably throttled :-(")
                return {'lat': None, 'lng':None}



        collections = {'fio': {
                'name': 'bsowens_ggelinas.fio',
                'unset': {'location':''},
                'loc': coord_query
            }}


        print("Running...")
        for doc in repo['bsowens_ggelinas.fio'].find():
            collection = collections['fio']
            if 'coords' in doc:
                #skip this iteration if the data is already present
                continue
            coords = coord_query(doc)
            repo['bsowens_ggelinas.fio'].update(
                {'_id': doc['_id']},
                {
                    '$set': {'coords': coords},
                    '$unset': collection['unset']
                },
                upsert=False
            )
        print('Done finding coordinates')
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}




    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bsowens_ggelinas', 'bsowens_ggelinas')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/bsowens_ggelinas')
        # The scripts are in <folder>#<filename> format.

        doc.add_namespace('dat', 'http://datamechanics.io/data/bsowens_ggelinas')
        # The data sets are in <user>#<collection> format.

        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.

        doc.add_namespace('log', 'http://datamechanics.io/log/')
        # The event log.

        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bsowens_ggelinas#getFIOcoord',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:bsowens_ggelinas#fio',
                              {'prov:label': 'Field Interrogation Observations',
                               prov.model.PROV_TYPE: 'ont:DataSet'})
        this_run = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                  {'prov:label': 'Longitude and Latitude Coordinates'})
        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(
            this_run,
            resource,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        fio = doc.entity('dat:bsowens_ggelinas#getFIOcoord', {prov.model.PROV_LABEL:'FIO with Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(resource, this_script)
        doc.wasGeneratedBy(resource, this_run, endTime)
        doc.wasDerivedFrom(resource, resource, this_run, this_run, this_run)


        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc

getFIOcoord.execute()
doc = getFIOcoord.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize(), indent=4)))
