import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'ktan_ngurung'
    reads = []
    writes = ['ktan_ngurung.bigBelly', 'ktan_ngurung.colleges', 'ktan_ngurung.hubways', 'ktan_ngurung.busStops', 'ktan_ngurung.tStops', 'ktan_ngurung.ridership']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')

        url = 'https://raw.githubusercontent.com/ktango/course-2016-fal-proj/master/data-files/big-belly-locations.json'
        response = urllib.request.urlopen(url).read().decode('utf-8')
        r0 = json.loads(response)
        s0 = json.dumps(r0, sort_keys=True, indent=2)
        repo.dropPermanent("bigBelly")
        repo.createPermanent("bigBelly")
        repo['ktan_ngurung.bigBelly'].insert_one(r0)

        url = 'https://raw.githubusercontent.com/ktango/course-2016-fal-proj/master/data-files/colleges-and-universities.json'
        response = urllib.request.urlopen(url).read().decode('utf-8')
        r1 = json.loads(response)
        s1 = json.dumps(r1, sort_keys=True, indent=2)
        repo.dropPermanent("colleges")
        repo.createPermanent("colleges")
        repo['ktan_ngurung.colleges'].insert_many(r1)

        url = 'https://raw.githubusercontent.com/ktango/course-2016-fal-proj/master/data-files/hubway-stations-in-boston.json'
        response = urllib.request.urlopen(url).read().decode('utf-8')
        r2 = json.loads(response)
        s2 = json.dumps(r2, sort_keys=True, indent=2)
        repo.dropPermanent("hubways")
        repo.createPermanent("hubways")
        repo['ktan_ngurung.hubways'].insert_many(r2)

        url = 'https://raw.githubusercontent.com/ktango/course-2016-fal-proj/master/data-files/mbta-bus-stops.json'
        response = urllib.request.urlopen(url).read().decode('utf-8')
        r3 = json.loads(response)
        s3 = json.dumps(r3, sort_keys=True, indent=2)
        repo.dropPermanent("busStops")
        repo.createPermanent("busStops")
        repo['ktan_ngurung.busStops'].insert_many(r3)

        url = 'https://raw.githubusercontent.com/ktango/course-2016-fal-proj/master/data-files/t-stop-locations.json'
        response = urllib.request.urlopen(url).read().decode('utf-8')
        r4 = json.loads(response)
        s4 = json.dumps(r4, sort_keys=True, indent=2)
        repo.dropPermanent("tStops")
        repo.createPermanent("tStops")
        repo['ktan_ngurung.tStops'].insert_many(r4)

        url = 'https://raw.githubusercontent.com/ktango/course-2016-fal-proj/master/data-files/boston-ridership.json'
        response = urllib.request.urlopen(url).read().decode('utf-8')
        r5 = json.loads(response)
        s5 = json.dumps(r5, sort_keys=True, indent=2)
        repo.dropPermanent("ridership")
        repo.createPermanent("ridership")
        repo['ktan_ngurung.ridership'].insert_many(r5)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

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
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('ods', 'https://boston.opendatasoft.com/api/records/1.0/search/?dataset=')
        doc.add_namespace('ede', 'http://erikdemaine.org/maps/')
        doc.add_namespace('mbt', 'http://www.mbta.com/about_the_mbta/document_library/')

        this_script = doc.agent('alg:ktan_ngurung#landmarkLocations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        bigBelly_resource = doc.entity('bdp:42qi-w8d7', {'prov:label':'Big Belly Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        colleges_resource = doc.entity('ods:colleges-and-universities', {'prov:label':'Colleges and Universities', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        hubways_resource = doc.entity('ods:hubway-stations-in-boston', {'prov:label':'Hubway Stations in Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        busStops_resource = doc.entity('ods:mbta-bus-stops&facet=town', {'prov:label':'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        tStops_resource = doc.entity('ede:mbta', {'prov:label':'T-Stop Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'yaml'})
        ridership_resource = doc.entity('mbt:?search=blue+book&submit_document_search=Search+Library', {'prov:label':'Boston 2014 Bluebook', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'pdf'})

        get_bigBelly = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_colleges = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_hubways = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_busStops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_tStops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_ridership = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
       
        doc.wasAssociatedWith(get_bigBelly, this_script)
        doc.wasAssociatedWith(get_colleges, this_script)
        doc.wasAssociatedWith(get_hubways, this_script)
        doc.wasAssociatedWith(get_busStops, this_script)
        doc.wasAssociatedWith(get_tStops, this_script)
        doc.wasAssociatedWith(get_ridership, this_script)

        doc.usage(get_bigBelly, bigBelly_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        doc.usage(get_colleges, colleges_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        doc.usage(get_hubways, hubways_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        doc.usage(get_busStops, busStops_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        doc.usage(get_tStops, tStops_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        doc.usage(get_ridership, ridership_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        bigBelly = doc.entity('dat:ktan_ngurung#bigBelly', {prov.model.PROV_LABEL:'Big Belly Locations Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bigBelly, this_script)
        doc.wasGeneratedBy(bigBelly, get_bigBelly, endTime)
        doc.wasDerivedFrom(bigBelly, bigBelly_resource, get_bigBelly, get_bigBelly, get_bigBelly)

        colleges = doc.entity('dat:ktan_ngurung#colleges', {prov.model.PROV_LABEL:'Colleges and Universities Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(colleges, this_script)
        doc.wasGeneratedBy(colleges, get_colleges, endTime)
        doc.wasDerivedFrom(colleges, colleges_resource, get_colleges, get_colleges, get_colleges)

        hubways = doc.entity('dat:ktan_ngurung#hubways', {prov.model.PROV_LABEL:'Hubway Stations in Boston Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hubways, this_script)
        doc.wasGeneratedBy(hubways, get_hubways, endTime)
        doc.wasDerivedFrom(hubways, hubways_resource, get_hubways, get_hubways, get_hubways)

        busStops = doc.entity('dat:ktan_ngurung#busStops', {prov.model.PROV_LABEL:'MBTA Bus Stops Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(busStops, this_script)
        doc.wasGeneratedBy(busStops, get_busStops, endTime)
        doc.wasDerivedFrom(busStops, busStops_resource, get_busStops, get_busStops, get_busStops)

        tStops = doc.entity('dat:ktan_ngurung#tStops', {prov.model.PROV_LABEL:'T-Stop Locations Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(tStops, this_script)
        doc.wasGeneratedBy(tStops, get_tStops, endTime)
        doc.wasDerivedFrom(tStops, tStops_resource, get_tStops, get_tStops, get_tStops)

        ridership = doc.entity('dat:ktan_ngurung#ridership', {prov.model.PROV_LABEL:'Boston T Ridership Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(ridership, this_script)
        doc.wasGeneratedBy(ridership, get_ridership, endTime)
        doc.wasDerivedFrom(ridership, ridership_resource, get_ridership, get_ridership, get_ridership)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc 


example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof