import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import geocoder
from collections import Counter

class collegeBusStops(dml.Algorithm):
    contributor = 'ktan_ngurung_yazhang_emilyh23'
    reads = ['ktan_ngurung_yazhang_emilyh23.colleges', 'ktan_ngurung_yazhang_emilyh23.busStops']
    writes = ['ktan_ngurung_yazhang_emilyh23.collegeBusStopCounts']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')

        # Get bus stop and college location data
        busStops = repo.ktan_ngurung_yazhang_emilyh23.busStops.find() 
        colleges = repo.ktan_ngurung_yazhang_emilyh23.colleges.find()
        collegeAndStopCountsDict = {}

        # Commented block below can only be a when query limit for geocoder library has not been used up
        '''
        for stop in busStops:

            coordinates = stop['fields']['geo_point_2d']
            address = geocoder.google(coordinates, method='reverse')
            zipcode = str(address.postal)

            if zipcode != 'None' and zipcode not in collegeAndStopCountsDict:
                collegeAndStopCountsDict[zipcode] = {'busStopCount' : 1, 'collegeCount' : 0}

            elif zipcode != 'None' and zipcode in collegeAndStopCountsDict:
                collegeAndStopCountsDict[zipcode]['busStopCount'] += 1

            else:
                pass

        for college in colleges:

            coordinates = college['fields']['geo_point_2d']
            address = geocoder.google(coordinates, method='reverse')
            zipcode = str(address.postal)

            if zipcode != 'None' and zipcode in collegeAndStopCountsDict:
                collegeAndStopCountsDict[zipcode]['collegeCount'] += 1

            elif zipcode != 'None' and zipcode not in collegeAndStopCountsDict:
                collegeAndStopCountsDict[zipcode] = {'busStopCount' : 0, 'collegeCount': 1}

            else:
                pass
        '''

        # College count dictionary
        c = {'02132': {'busStopCount': 0, 'collegeCount': 1}, '02136': {'busStopCount': 0, 'collegeCount': 1}, '02124': {'busStopCount': 0, 'collegeCount': 1}, '02467': {'busStopCount': 0, 'collegeCount': 1}, '02111': {'busStopCount': 0, 'collegeCount': 3}, '02116': {'busStopCount': 0, 'collegeCount': 7}, '02163': {'busStopCount': 0, 'collegeCount': 2}, '02130': {'busStopCount': 0, 'collegeCount': 1}, '02129': {'busStopCount': 0, 'collegeCount': 3}, '02119': {'busStopCount': 0, 'collegeCount': 1}, '02115': {'busStopCount': 0, 'collegeCount': 12}, '02110': {'busStopCount': 0, 'collegeCount': 1}, '02108': {'busStopCount': 0, 'collegeCount': 1}, '02118': {'busStopCount': 0, 'collegeCount': 2}, '02125': {'busStopCount': 0, 'collegeCount': 1}, '02114': {'busStopCount': 0, 'collegeCount': 1}, '02113': {'busStopCount': 0, 'collegeCount': 1}, '02135': {'busStopCount': 0, 'collegeCount': 2}, '02120': {'busStopCount': 0, 'collegeCount': 1}, '02215': {'busStopCount': 0, 'collegeCount': 17}}
        # Bus count dictionary
        b = {'02114': {'busStopCount': 11, 'collegeCount': 0}, '02109': {'busStopCount': 13, 'collegeCount': 0}, '02120': {'busStopCount': 42, 'collegeCount': 0}, '02108': {'busStopCount': 4, 'collegeCount': 0}, '02131': {'busStopCount': 139, 'collegeCount': 0}, '02121': {'busStopCount': 88, 'collegeCount': 0}, '02115': {'busStopCount': 47, 'collegeCount': 0}, '02203': {'busStopCount': 3, 'collegeCount': 0}, '02110': {'busStopCount': 13, 'collegeCount': 0}, '02133': {'busStopCount': 1, 'collegeCount': 0}, '02199': {'busStopCount': 3, 'collegeCount': 0}, '02215': {'busStopCount': 52, 'collegeCount': 0}, '02125': {'busStopCount': 97, 'collegeCount': 0}, '02122': {'busStopCount': 87, 'collegeCount': 0}, '02136': {'busStopCount': 171, 'collegeCount': 0}, '02118': {'busStopCount': 92, 'collegeCount': 0}, '02446': {'busStopCount': 6, 'collegeCount': 0}, '02210': {'busStopCount': 42, 'collegeCount': 0}, '02124': {'busStopCount': 165, 'collegeCount': 0}, '02467': {'busStopCount': 4, 'collegeCount': 0}, '02134': {'busStopCount': 60, 'collegeCount': 0}, '02132': {'busStopCount': 186, 'collegeCount': 0}, '02129': {'busStopCount': 65, 'collegeCount': 0}, '02116': {'busStopCount': 31, 'collegeCount': 0}, '02149': {'busStopCount': 1, 'collegeCount': 0}, '02126': {'busStopCount': 85, 'collegeCount': 0}, '02163': {'busStopCount': 3, 'collegeCount': 0}, '02119': {'busStopCount': 119, 'collegeCount': 0}, '02130': {'busStopCount': 110, 'collegeCount': 0}, '02171': {'busStopCount': 1, 'collegeCount': 0}, '02128': {'busStopCount': 112, 'collegeCount': 60}, '02135': {'busStopCount': 113, 'collegeCount': 0}, '02111': {'busStopCount': 12, 'collegeCount': 0}, '02127': {'busStopCount': 128, 'collegeCount': 0}}

        # Merging dictionaries above
        for zipcode, value in c.items():
            c[zipcode] = Counter(value)

        for zipcode, value in b.items():
            b[zipcode] = Counter(value)

        for zipcode, value in c.items():
            if zipcode in b.keys():
                collegeAndStopCountsDict[zipcode] = b[zipcode] + c[zipcode]
            else:
                collegeAndStopCountsDict[zipcode] = c[zipcode]

        for zipcode, value in b.items():
            if zipcode not in collegeAndStopCountsDict:
                collegeAndStopCountsDict[zipcode] = b[zipcode]

        for zipcode, value in collegeAndStopCountsDict.items():
            collegeAndStopCountsDict[zipcode] = dict(value)

        # Hard coded dictionary built from above code in variable collection due to geocoder query limit
        collegeAndStopCountsDict = {'02203': {'busStopCount': 3, 'collegeCount': 0}, '02114': {'busStopCount': 11, 'collegeCount': 1}, '02171': {'busStopCount': 1, 'collegeCount': 0}, '02129': {'busStopCount': 65, 'collegeCount': 3}, '02132': {'busStopCount': 186, 'collegeCount': 1}, '02113': {'busStopCount': 0, 'collegeCount': 1}, '02131': {'busStopCount': 139, 'collegeCount': 0}, '02149': {'busStopCount': 1, 'collegeCount': 0}, '02120': {'busStopCount': 42, 'collegeCount': 1}, '02128': {'busStopCount': 112, 'collegeCount': 60}, '02134': {'busStopCount': 60, 'collegeCount': 0}, '02108': {'busStopCount': 4, 'collegeCount': 1}, '02467': {'busStopCount': 4, 'collegeCount': 1}, '02127': {'busStopCount': 128, 'collegeCount': 0}, '02121': {'busStopCount': 88, 'collegeCount': 0}, '02136': {'busStopCount': 171, 'collegeCount': 1}, '02210': {'busStopCount': 42, 'collegeCount': 0}, '02109': {'busStopCount': 13, 'collegeCount': 0}, '02215': {'busStopCount': 52, 'collegeCount': 17}, '02118': {'busStopCount': 92, 'collegeCount': 2}, '02122': {'busStopCount': 87, 'collegeCount': 0}, '02163': {'busStopCount': 3, 'collegeCount': 2}, '02126': {'busStopCount': 85, 'collegeCount': 0}, '02116': {'busStopCount': 31, 'collegeCount': 7}, '02130': {'busStopCount': 110, 'collegeCount': 1}, '02199': {'busStopCount': 3, 'collegeCount': 0}, '02110': {'busStopCount': 13, 'collegeCount': 1}, '02115': {'busStopCount': 47, 'collegeCount': 12}, '02446': {'busStopCount': 6, 'collegeCount': 0}, '02111': {'busStopCount': 12, 'collegeCount': 3}, '02125': {'busStopCount': 97, 'collegeCount': 1}, '02119': {'busStopCount': 119, 'collegeCount': 1}, '02135': {'busStopCount': 113, 'collegeCount': 2}, '02124': {'busStopCount': 165, 'collegeCount': 1}, '02133': {'busStopCount': 1, 'collegeCount': 0}}
        
        # Convert dictionary into JSON object 
        data = json.dumps(collegeAndStopCountsDict, sort_keys=True, indent=2)
        r = json.loads(data)

        # Create new dataset called collegeBusStopCounts
        repo.dropPermanent("collegeBusStopCounts")
        repo.createPermanent("collegeBusStopCounts")
        repo['ktan_ngurung_yazhang_emilyh23.collegeBusStopCounts'].insert_one(r)

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
        repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:ktan_ngurung_yazhang_emilyh23#collegeBusStops', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        colleges_resource = doc.entity('dat:ktan_ngurung_yazhang_emilyh23/colleges-and-universities', {'prov:label':'Colleges and Universities', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        busStops_resource = doc.entity('dat:ktan_ngurung_yazhang_emilyh23/mbta-bus-stops', {'prov:label':'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, colleges_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )
        doc.usage(this_run, busStops_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        collegeBusStops = doc.entity('dat:ktan_ngurung_yazhang_emilyh23#collegeBusStops', {prov.model.PROV_LABEL:'Number of Colleges And Bus Stops for Each Zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(collegeBusStops, this_script)
        doc.wasGeneratedBy(collegeBusStops, this_run, endTime)
        doc.wasDerivedFrom(collegeBusStops, colleges_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(collegeBusStops, busStops_resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

collegeBusStops.execute() 
doc = collegeBusStops.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

