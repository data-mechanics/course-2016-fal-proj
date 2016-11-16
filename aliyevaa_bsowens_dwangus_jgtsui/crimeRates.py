##takes sum of distances from all of the locations
##and multiplies them by the community value
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
import ast
import math




def calculate(x0, y0, x1, y1):
    x_d_sq = pow(abs(x0) - abs(x1), 2)
    y_d_sq = pow(abs(y0) - abs(y1), 2)
    dist = math.sqrt(x_d_sq + y_d_sq)
    return dist


class crimeRates(dml.Algorithm):
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
    reads = ['crime']

    titles = ['Crime Rate Cluster']

    setExtensions = ['crimeRates']

    urls = ['https://data.cityofboston.gov/resource/ufcx-3fdn.json']


    writes = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in setExtensions]

    dataSetDict = {}
    for i in range(len(setExtensions)):
        dataSetDict[setExtensions[i]] = (urls[i], writes[i], titles[i], urls[i][39:48])

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(crimeRates.contributor, crimeRates.contributor)
        lines = [line.rstrip('\n') for line in open('centers.txt')]
        coordinates = []
        point = []
        for line in lines:
            if line != '':
                point = [float(x) for x in line.split()]
                coordinates.append(point)
                point = []
        out = open('crime_spots.txt', 'w')

        entry = dict((str(center[0]) + ' ' + str(center[1]),0) for center in coordinates)
        score = 0
        count = 0
        # json.dump(x, out), x-object
        elem_n = 0
        print("# coordinates: " + str(len(coordinates)))
        repo.dropPermanent("crimeRates")
        repo.createPermanent("crimeRates")

        for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.crime2012_2015.find():
            elem_n += 1
            this_coord = [elem['location']['coordinates'][0],elem['location']['coordinates'][1]]

            # find closest center

            closest_so_far = 100000000
            for center in coordinates:
                c_str = str(center[0]) + ' ' + str(center[1])
                d = calculate(this_coord[0],
                              this_coord[1],
                              center[0],
                              center[1])
                if d < closest_so_far:
                    closest_so_far = d
                    closest_center = c_str
            entry[closest_center] += 1
            count_stuff += 1
            if elem_n % 100 == 0:
                print("Parsed " +  str(elem_n) + " crime entries")


        # lmfao @ how inefficient this is I LOVE FOR LOOPS OK

        for el in entry.keys():
            repo.aliyevaa_bsowens_dwangus_jgtsui.crimeRates.insert({str(el) : str(entry[el])},check_keys=False)

        json.dump(entry, out)
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(crimeRates.contributor, crimeRates.contributor)
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://maps.googleapis.com/maps/api/place')

        this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#crimeRates',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        for key in crimeRates.dataSetDict.keys():
            resource = doc.entity('bdp:' + crimeRates.dataSetDict[key][3], {'prov:label':crimeRates.dataSetDict[key][2], prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
            get_something = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(get_something, this_script)
            something = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#' + key, {prov.model.PROV_LABEL:crimeRates.dataSetDict[key][2], prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(something, this_script)
            doc.wasGeneratedBy(something, get_something, endTime)
            doc.wasDerivedFrom(something, resource, get_something, get_something, get_something)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


crimeRates.execute()
doc = crimeRates.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))
