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

    setExtensions = ['boston_grid_crime_rates_cellSize1000sqft']
    titles = ['Boston Grid Cells Crime Incidence 2012 - 2015']
    oldSetExtensions = ['crime2012_2015', 'cell_GPS_center_coordinates']

    reads = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in oldSetExtensions]
    writes = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in setExtensions]

    dataSetDict = {}
    for i in range(len(setExtensions)):
        dataSetDict[setExtensions[i]] = (writes[i], titles[i])

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(crimeRates.contributor, crimeRates.contributor)

        myrepo = repo.aliyevaa_bsowens_dwangus_jgtsui

        '''
        cellCoordinates = myrepo[crimeRates.reads[1]]
        coordinates = []
        for cellCoord in cellCoordinates.find():
            coordinates.append((cellCoord['longitude'],cellCoord['latitude']))
        #'''

        #'''
        lines = [line.rstrip('\n') for line in open('centers.txt')]
        coordinates = []
        for line in lines:
            if line != '':
                coordinates.append([float(x) for x in line.split()])
        #'''

        entry = dict((str(center[0]) + ' ' + str(center[1]),0) for center in coordinates)
        print("# Total Cell Coordinates: {}".format(len(coordinates)))

        repo.dropPermanent(crimeRates.setExtensions[0])
        repo.createPermanent(crimeRates.setExtensions[0])

        crimeDataSet = myrepo[crimeRates.reads[0]]
        print(crimeDataSet.count())
        count = 0
        for elem in crimeDataSet.find():
            if ('location' in elem) and ('coordinates' in elem['location']) \
                    and (elem['location']['coordinates'][0] > 0) \
                    and (elem['location']['coordinates'][1] > 0):
                count += 1

                long = elem['location']['coordinates'][0]
                lat = elem['location']['coordinates'][1]

                #Find closest cell-center to this particular crime's GPS coordinates
                asdasd = coordinates[0][0]
                qwuwq = coordinates[0][1]
                closest_so_far = calculate(long, lat, coordinates[0][0], coordinates[0][1])
                closest_center = str(coordinates[0][0]) + ' ' + str(coordinates[0][1])
                for center in coordinates[1:]:
                    c_str = str(center[0]) + ' ' + str(center[1])
                    d = calculate(long, lat, center[0], center[1])
                    if d < closest_so_far:
                        closest_so_far = d
                        closest_center = c_str

                entry[closest_center] += 1
                if count % 100 == 0:
                    print("Parsed " +  str(count) + " crime entries")

        for e in entry.keys():
            crimeDataSet.insert({str(e): entry[e]}, check_keys=False)
            #crimeDataSet.insert({str(e): str(entry[e])}, check_keys=False)

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
            get_something = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(get_something, this_script)
            something = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#' + key, {prov.model.PROV_LABEL:crimeRates.dataSetDict[key][1], prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(something, this_script)
            doc.wasGeneratedBy(something, get_something, endTime)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()
        return doc


crimeRates.execute()
doc = crimeRates.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))