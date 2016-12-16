import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
import math

class getNeighborhoodScores(dml.Algorithm):
    contributor = 'jyaang_robinliu106'
    reads = []
    writes = ['jyaang_robinliu106.hospital_coord', 'jyaang_robinliu106.school_coord', 'jyaang_robinliu106.DayCamp_coord', 'jyaang_robinliu106.neighborhood_scores']

    @staticmethod
    def execute(trial = False):

        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        Hospital_coord = []
        Hospital_cursor = repo['jyaang_robinliu106.hospital'].find()
        for entry in Hospital_cursor:
            n = entry['hospitalName']
            c = entry['coord']
            Hospital_coord.append([n,c])
            # Ex. [ "Lemuel Shattuck Hospital", [ "42.30025000839615", "-71.10737910445549" ] ]

        ### DEMO DEMO DEMO
        json.dump(Hospital_coord, open('hospital.json', 'w'))


        School_coord = []
        School_cursor = repo['jyaang_robinliu106.school'].find()
        for entry in School_cursor:
            n = entry['schoolName']
            c = entry['coord']
            School_coord.append([n,c])
        #repo.dropPermanent("school_coord")
        #repo.createPermanent("school_coord")
        #repo['jyaang_robinliu106.school_coord'].insert_many(School_coord)

        DayCamp_coord = []
        DayCamp_cursor = repo['jyaang_robinliu106.dayCamp'].find()
        for entry in DayCamp_cursor:
            n = entry['campName']
            c = entry['coord']
            DayCamp_coord.append([n,c])
        #repo.dropPermanent("dayCamp_coord")
        #repo.createPermanent("dayCamp_coord")
        #repo['jyaang_robinliu106.dayCamp_coord'].insert_many(DayCamp_coord)

        Crime_coord = []
        Crime_cursor = repo['jyaang_robinliu106.crime'].find()
        for entry in Crime_cursor:
            n = entry['crimeName']
            c = entry['coord']
            Crime_coord.append([n,c])
        #repo.dropPermanent("crime_coord")
        #repo.createPermanent("crime_coord")
        #repo["jyaang_robinliu106.crime_coord"].insert(Crime_coord)

        Property_coord = []
        Property_cursor = repo['jyaang_robinliu106.property'].find()
        for entry in Property_cursor:
            n = entry['LU']
            c = entry['coord']
            v = entry['value']
            Property_coord.append([n,c,v])
            #print(Property_coord)

        neighborhoods = [
        ['Allston', [42.3539, -71.1337]],
        ['Back Bay', [42.3503, -71.0810]],
        ['Bay Village', [42.3490, -71.0698]],
        ['Beacon Hill', [42.3588, -71.0707]],
        ['Brighton', [42.3464, -71.1627]],
        ['Charlestown', [42.3782, -71.0602]],
        ['Chinatown', [42.3501, -71.0624]],
        ['Dorchester', [42.3016, -71.0676]],
        ['Downtown Crossing', [42.3555, -71.0594]],
        ['East Boston', [42.3702, -71.0389]],
        ['Fenway', [42.3429, -71.1003]],
        ['Hyde Park', [42.2565, -71.1241]],
        ['Jamaica Plain', [42.3097, -71.0476]],
        ['Mattapan', [42.2771, -71.0914]],
        ['Mission Hill', [42.3296, -71.1062]],
        ['North End', [42.3647, -71.0542]],
        ['Roslindale', [42.2832, -71.1270]],
        ['Roxbury', [42.3152, -71.0914]],
        ['South Boston', [42.3381, -71.0476]],
        ['South End', [42.3388, -71.0765]],
        ['West End', [42.3644, -71.0661]],
        ['West Roxbury', [42.2798, -71.1627]]
        ]

        def getDistance(lat1,lon1,lat2,lon2):
          R = 6371
          dLat = deg2rad(lat2-lat1)
          dLon = deg2rad(lon2-lon1)
          a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(deg2rad(lat1)) * math.cos(deg2rad(lat2)) * math.sin(dLon/2) * math.sin(dLon/2)
          c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
          d = R * c
          return d


        def deg2rad(deg):
          return deg * (math.pi/180)

        # Optimizing distance by min (optimization problem)
        def minHospital(category):
            if (trial):
                category = category[:5]

            distancesPerCity = [0] * len(neighborhoods)


            for i in range(len(neighborhoods)):
                minDistance = 1000000
                for t in range(len(category)):
                    currentDistance = getDistance( neighborhoods[i][1][0] , neighborhoods[i][1][1] , float(category[t][1][0]), float(category[t][1][1]) )
                    if currentDistance < minDistance:
                        minDistance = currentDistance
                        distancesPerCity[i] = [neighborhoods[i],category[t],minDistance]

            return distancesPerCity

        # Constraint using threshold
        def countCategory(category):
            if (trial):
                category = category[:5]

            countPerCity = [0] * len(neighborhoods)
            threshold = 3

            for i in range(len(neighborhoods)):
                count = 0

                for t in range(len(category)):
                    currentDistance = getDistance( neighborhoods[i][1][0] , neighborhoods[i][1][1] , float(category[t][1][0]), float(category[t][1][1]) )
                    if currentDistance < threshold: #category is within 3km of the neighborhood
                        count += 1
                        countPerCity[i] = [neighborhoods[i],count] #update count for respective neighborhood
                if count == 0:
                    countPerCity[i] = [neighborhoods[i],count]

            return countPerCity

        def propCalc(data):
            if (trial):
                data = data[:5]

            countPerCity = [0] * len(neighborhoods)
            threshold = 3

            for i in range(len(neighborhoods)):
                calc = 0
                count = 0

                for t in range(len(data)):
                    currentDistance = getDistance(neighborhoods[i][1][0], neighborhoods[i][1][1], float(data[t][1][0]), float(data[t][1][1]))
                    if currentDistance < threshold: # the property is within 3km radius
                        calc += int(data[t][-1])
                        count += 1
                        countPerCity[i] = [neighborhoods[i], calc]
                if calc == 0:
                    countPerCity[i] = [neighborhoods[i], calc]
                countPerCity[i] = [neighborhoods[i], calc//count]
                # divide total value of all nearby residence properties by the number of nearby residence properties
            return countPerCity

        #count of each category per neighborhood
        crime_Count = countCategory(Crime_coord)
        hospital_Count = minHospital(Hospital_coord)
        #print(hospital_Count)
        school_Count = countCategory(School_coord)
        prop_Count = propCalc(Property_coord)
        #print(prop_Count)


        result = [[x for x in range(2)] for y in range(len(neighborhoods))]

        a = []
        # Scoring algorithm
        for i in range(len(neighborhoods)):
            result[i][0] = neighborhoods[i][0]

            # Calculate score
            result[i][1] = hospital_Count[i][-1] * 0.25
            result[i][1] += school_Count[i][-1] * 0.25
            result[i][1] -= crime_Count[i][-1] * 0.25
            result[i][1] -= prop_Count[i][-1] * 0.25
            a.append({'neighborhood' : result[i][0], 'hosptial_count': hospital_Count[i][-1], 'school_count': school_Count[i][-1], 'crime_rate': crime_Count[i][-1], 'property_value': prop_Count[i][-1], 'score': ((-1)/result[i][1]) * 1000000})


        repo.dropPermanent("neighborhood_scores")
        repo.createPermanent("neighborhood_scores")
        repo['jyaang_robinliu106.neighborhood_scores'].insert_many(a)
        endTime = datetime.datetime.now()
        repo.logout()
        return {"start":startTime, "end":endTime}



    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        """
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        """

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jyaang_robinliu106#getNeighborhoodScores', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        hospital_resource = doc.entity('bdp:46f7-2snz', {'prov:label': 'Hospital Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        school_resource = doc.entity('bdp:e29s-ympv', {'prov:label': 'School Coordinates', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':'json'})
        crime_resource = doc.entity('bdp:fqn4-4qap', {'prov:label': 'Crime Coordinates', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension':'json'})
        neighborhoodScores = doc.entity('dat:jyaang_robinliu106#neighborhood_scores', {prov.model.PROV_LABEL: 'Scores of each Boston neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
        get_NeighborhoodScores = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(get_NeighborhoodScores, this_script)
        doc.used(get_NeighborhoodScores, neighborhoodScores, startTime)
        doc.wasAttributedTo(neighborhoodScores, this_script)
        doc.wasGeneratedBy(neighborhoodScores, get_NeighborhoodScores, endTime)

        repo.record(doc.serialize()) # Record the provenance document
        repo.logout()
        return doc


getNeighborhoodScores.execute(trial=False)
doc = getNeighborhoodScores.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
