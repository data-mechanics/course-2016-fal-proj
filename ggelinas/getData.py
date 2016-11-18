import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getData(dml.Algorithm):
    contributor = 'alice_bob'
    reads = []
    writes = ['ggelinas.stations',
              'ggelinas.incidents',
              'ggelinas.property',
              'ggelinas.fio',
              'ggelinas.hospitals']

    @staticmethod
    def execute(trial = False):
        '''Retrieve locations of BPD district stations'''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ggelinas', 'ggelinas')

        dataSets = {
            'stations': 'https://data.cityofboston.gov/resource/pyxn-r3i2.json',
            'incidents': 'https://data.cityofboston.gov/resource/29yf-ye7n.json',
            'property':'https://data.cityofboston.gov/resource/g5b5-xrwi.json',
            'fio':'https://data.cityofboston.gov/resource/2pem-965w.json',
            'hospitals': 'https://data.cityofboston.gov/resource/u6fv-m8v4.json'
        }

        for set in dataSets:

            print("Downloading dataset: ",set)
            url = dataSets[set]
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            repo.dropPermanent(set)
            repo.createPermanent(set)
            repo['ggelinas.' + set].insert_many(r)
            print('Done!')


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start:startTime", "end:endTime"}

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
        repo.authenticate('ggelinas', 'ggelinas')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/ggelinas') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ggelinas#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        stations_info = doc.entity('bdp:pyxn-r3i2', {'prov:label':'District Police Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        stations_getInfo = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get District Police Stations Data'})
        doc.wasAssociatedWith(stations_getInfo, this_script)
        doc.usage(
            stations_getInfo,
            stations_info,
            startTime,
            None,
            {prov.model.PROV_TYPE:'ont:Retrieval'}
        )

        incidents_info = doc.entity('bdp:29yf-ye7n',
                                   {'prov:label': 'Crime Incidents Report', prov.model.PROV_TYPE: 'ont:DataResource',
                                    'ont:Extension': 'json'})
        incidents_getInfo = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                        {'prov:label': 'Get Crime Incidents Report Data'})
        doc.wasAssociatedWith(incidents_getInfo, this_script)
        doc.usage(
            incidents_getInfo,
            incidents_info,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Retrieval'}
        )

        property_info = doc.entity('bdp:g5b5-xrwi',
                                    {'prov:label': 'Property Assessment 2016', prov.model.PROV_TYPE: 'ont:DataResource',
                                     'ont:Extension': 'json'})
        property_getInfo = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                         {'prov:label': 'Get Property Assessment 2016 Data'})
        doc.wasAssociatedWith(property_getInfo, this_script)
        doc.usage(
            property_getInfo,
            property_info,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Retrieval'}
        )

        fio_info = doc.entity('bdp:2pem-965w',
                                   {'prov:label': 'Boston Police Department FIO', prov.model.PROV_TYPE: 'ont:DataResource',
                                    'ont:Extension': 'json'})
        fio_getInfo = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                        {'prov:label': 'Get Boston Police Department FIO Data'})
        doc.wasAssociatedWith(fio_getInfo, this_script)
        doc.usage(
            fio_getInfo,
            fio_info,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Retrieval'}
        )

        hospitals_info = doc.entity('bdp:u6fv-m8v4',
                              {'prov:label': 'Hospital Locations', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        hospitals_getInfo = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                   {'prov:label': 'Get Hospital Locations Data'})
        doc.wasAssociatedWith(hospitals_getInfo, this_script)
        doc.usage(
            hospitals_getInfo,
            hospitals_info,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Retrieval'}
        )

        stations = doc.entity('dat:ggelinas#stations',
                               {prov.model.PROV_LABEL: 'Boston Police Stations District', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, incidents_getInfo, endTime)
        doc.wasDerivedFrom(stations, incidents_info, incidents_getInfo, incidents_getInfo, incidents_getInfo)

        incidents = doc.entity('dat:ggelinas#incidents',
                              {prov.model.PROV_LABEL: 'Crime Incidents Report', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(incidents, this_script)
        doc.wasGeneratedBy(incidents, incidents_getInfo, endTime)
        doc.wasDerivedFrom(incidents, incidents_info, incidents_getInfo, incidents_getInfo, incidents_getInfo)

        property = doc.entity('dat:ggelinas#property',
                         {prov.model.PROV_LABEL: 'Property Assessment 2016', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(property, this_script)
        doc.wasGeneratedBy(property, property_getInfo, endTime)
        doc.wasDerivedFrom(property, property_info, property_getInfo, property_getInfo, property_getInfo)

        fio = doc.entity('dat:ggelinas#fio',
                               {prov.model.PROV_LABEL: 'Boston Police Department FIO', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(fio, this_script)
        doc.wasGeneratedBy(fio, fio_getInfo, endTime)
        doc.wasDerivedFrom(fio, fio_info, fio_getInfo, fio_getInfo, fio_getInfo)

        hospitals = doc.entity('dat:ggelinas#hospitals', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospitals, this_script)
        doc.wasGeneratedBy(hospitals, hospitals_getInfo, endTime)
        doc.wasDerivedFrom(hospitals, hospitals_info, hospitals_getInfo, hospitals_getInfo, hospitals_getInfo)

        repo.record(doc.serialize())
        repo.logout()

        return doc


getData.execute()
doc = getData.provenance()
#print(json.dumps(json.loads(doc.serialize()),indent=4))

#eof