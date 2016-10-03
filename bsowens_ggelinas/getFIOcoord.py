import urllib.request
import urllib.parse
import json
import dml
import prov.model
import datetime
import uuid


class getFIOcoord(dml.Algorithm):
    contributor = 'bsowens_ggelinas'
    reads = ['bsowens_ggelinas.stations',
              'bsowens_ggelinas.incidents',
              'bsowens_ggelinas.property',
              'bsowens_ggelinas.fio',
              'bsowens_ggelinas.hospitals']
    writes = ['bsowens_ggelinas.stations',
              'bsowens_ggelinas.incidents',
              'bsowens_ggelinas.property',
              'bsowens_ggelinas.fio',
              'bsowens_ggelinas.hospitals']

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
            url = "https://maps.googleapis.com/maps/api/geocode/json?address=" + address_str + "+MA&key=AIzaSyABZsMBMijsgiBXQuXMgUxs4fxxoxKXsX0"
            url = url.replace(" ", "+")
            urlres = urllib.request.urlopen(url).read().decode("utf-8")
            result = json.loads(urlres)
            return result['results'][0]['geometry']['location']



        collections = {'fio': {
                'name': 'bsowens_ggelinas.fio',
                'unset': {'location':''},
                'loc': coord_query
            }}




        for doc in repo['bsowens_ggelinas.fio'].find():
            if 'coords' in doc:
                #skip this iteration if the data is already present
                continue
            coords = coord_query(doc)

            repo[collection['name']].update(
                {'_id': doc['_id']},
                {
                    '$set': {'coords': coords},
                    '$unset': collection['unset']
                },
                upsert=False
            )
        print('Status: Completed collection: ', collection_name)

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

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        # The scripts are in <folder>#<filename> format.

        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        # The data sets are in <user>#<collection> format.

        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.

        doc.add_namespace('log', 'http://datamechanics.io/log/')
        # The event log.

        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bsowens_ggelinas#transformation1',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        standarize = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                  {'prov:label': 'Standarize geographical information'})
        doc.wasAssociatedWith(standarize, this_script)

        resource_stations = doc.entity('dat:bsowens_ggelinas#stations',
                                       {'prov:label': 'Police Station Locations',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            standarize,
            resource_stations,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_incidents = doc.entity('dat:bsowens_ggelinas#incidents',
                                        {'prov:label': 'Police Incident Reports',
                                         prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            standarize,
            resource_incidents,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_property = doc.entity('dat:bsowens_ggelinas#property',
                                       {'prov:label': 'Property Values', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            standarize,
            resource_property,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_fio = doc.entity('dat:bsowens_ggelinas#fio',
                                  {'prov:label': 'Field Interrogation Observations',
                                   prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            standarize,
            resource_fio,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        resource_hospitals = doc.entity('dat:bsowens_ggelinas#hospitals',
                                        {'prov:label': 'Hospital Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.usage(
            standarize,
            resource_hospitals,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        doc.wasAttributedTo(resource_stations, this_script)
        doc.wasAttributedTo(resource_incidents, this_script)
        doc.wasAttributedTo(resource_property, this_script)
        doc.wasAttributedTo(resource_fio, this_script)
        doc.wasAttributedTo(resource_hospitals, this_script)

        doc.wasGeneratedBy(resource_stations, standarize, endTime)
        doc.wasGeneratedBy(resource_incidents, standarize, endTime)
        doc.wasGeneratedBy(resource_property, standarize, endTime)
        doc.wasGeneratedBy(resource_fio, standarize, endTime)
        doc.wasGeneratedBy(resource_hospitals, standarize, endTime)

        doc.wasDerivedFrom(resource_stations, resource_stations, standarize, standarize, standarize)
        doc.wasDerivedFrom(resource_incidents, resource_incidents, standarize, standarize, standarize)
        doc.wasDerivedFrom(resource_property, resource_property, standarize, standarize, standarize)
        doc.wasDerivedFrom(resource_fio, resource_fio, standarize, standarize, standarize)
        doc.wasDerivedFrom(resource_hospitals, resource_hospitals, standarize, standarize, standarize)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc

getFIOcoord.execute()
doc = getFIOcoord.provenance()
