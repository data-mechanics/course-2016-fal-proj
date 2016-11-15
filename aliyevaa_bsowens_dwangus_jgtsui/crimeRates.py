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
    reads = ['aliyevaa_bsowens_dwangus_jgtsui.crime2012_2015']
    writes = ['aliyevaa_bsowens_dwangus_jgtsui.crimeRates']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(crimeRates.contributor, crimeRates.contributor)

        repo.dropPermanent("crimeRates")
        repo.createPermanent("crimeRates")
        lines = [line.rstrip('\n') for line in open('centers.txt')]
        coordinates = []
        point = []

        for line in lines:
            if line != '':
                point = [float(x) for x in line.split()]
                coordinates.append(point)
                point = []

        out = open('crime_spots.txt', 'w')
        entry = {}
        score = 0
        count = 0
        # json.dump(x, out), x-object
        elem_n = 0
        print("# coordinates: " + str(len(coordinates)))
        for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.crime2012_2015.find():
            elem_n += 1
            for center in coordinates:
                d = calculate(elem['location']['coordinates'][0], elem['location']['coordinates'][1], center[0],
                              center[1])
                score = d + score
                c = str(center[0]) + ' ' + str(center[1])
                entry.update({c: score})
                count = count + 1

            score = 0
        print(entry)
        print(elem_n)

        json.dump(entry, out)
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        return 0


crimeRates.execute()
doc = crimeRates.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))
