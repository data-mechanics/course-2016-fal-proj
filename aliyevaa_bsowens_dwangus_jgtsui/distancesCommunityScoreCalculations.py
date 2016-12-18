import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
import ast
import math

from scipy import spatial
import numpy as np


def calculate(x0,y0,x1,y1):
    x_d_sq=pow(abs(x0)-abs(x1),2)
    y_d_sq=pow(abs(y0)-abs(y1),2)
    dist=math.sqrt(x_d_sq+y_d_sq)
    return dist

class distancesCommunityScoreCalculations(dml.Algorithm):
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'

    oldSetExtensions = ['community_indicators', 'cell_GPS_center_coordinates']
    titles = ['Boston Grid Cells Inverse Community Score']
    setExtensions = ['boston_grid_community_values_cellSize1000sqft']

    reads = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in oldSetExtensions]
    writes = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in setExtensions]

    dataSetDict = {}
    for i in range(len(setExtensions)):
        dataSetDict[setExtensions[i]] = (writes[i], titles[i])
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(distancesCommunityScoreCalculations.contributor, distancesCommunityScoreCalculations.contributor)

        repo.dropPermanent("boston_grid_community_values_cellSize1000sqft")
        repo.createPermanent("boston_grid_community_values_cellSize1000sqft")

        myrepo = repo.aliyevaa_bsowens_dwangus_jgtsui

        '''
        cellCoordinates = repo[distancesCommunityScoreCalculations.reads[1]]
        coordinates = []
        for cellCoord in cellCoordinates.find():
            coordinates.append((cellCoord['longitude'], cellCoord['latitude']))
        #'''

        #'''
        # Edited by Jen -- since most of us don't have access to the db called 'cell_GPS_center_coordinates'
        # as we can't run gridCenters.py due to dependency issues

        # note: each line in centers.txt is in the form of
        # longitude latitude
        # where the coordinates are separated by a space.
        lines = [line.rstrip('\n') for line in open('centers.txt')]
        coordinates = []
        for line in lines:
            if line != '':
                point = [float(x) for x in line.split()]
                coordinates.append((point[0], point[1]))
        #'''

        score = 0
        prep = []
        ###David-added Code
        gpsDifference_1000Feet = 0.0028
        ###
        
        ###Faster way?
        commIndicatorsList = []
        for comIndicator in repo[distancesCommunityScoreCalculations.reads[0]].find():#Longitude, Latitude, Indicator Weight
            commIndicatorsList.append((comIndicator['location']['coordinates'][0], comIndicator['location']['coordinates'][1], comIndicator['community_score']))
        
        commRows = np.array([(tup[0], tup[1]) for tup in commIndicatorsList])
        centerCols = np.array(coordinates)

        '''
        #Looks like this:
        #(GPS Locations) cell-center cell-center cell-center ...
        #indicator
        #indicator
        #indicator
        #.
        #.
        #.
        '''

        Y = spatial.distance.cdist(commRows, centerCols, 'euclidean')
        
        newY = []
        for r in range(len(Y)):
            # Y[r] should be a vector corresponding to the GPS distancesCommunityScoreCalculations for each community indicator
            # commIndicatorsList[r][2] is the Indicator's weight in the original tuple
            # newY.append(Y[r]*commIndicatorsList[r][2])
            # Then multiply each indicator's distancesCommunityScoreCalculations to all cell-centers by that indicator's weight

            # Scaling in terms of feet!
            # ohhh -- imagine a very small distance between 2 GPS coordinates * small weight if negative indicator
            # --> then do the inverse of that
            newY.append(Y[r]*commIndicatorsList[r][2]/gpsDifference_1000Feet)

        # Then, we transpose the matrix, so that it's cell-centers on the rows, and indicators as the columns
        newY = np.array(newY).T

        # Then, we sum up (all the inverse (distance*weight) to every single community indicator) for a given
        # cell-center
        inverseDistances = [sum(1/newY[v]) for v in range(len(newY))] # --each v is a row-vector, then we take the
                                                                      # inverse distance of the entire vector to
                                                                      # produce a new vector, then we sum up over
                                                                      # that vector
        comScores = [(coordinates[y], inverseDistances[y]) for y in range(len(inverseDistances))]
        for x in comScores:
            entry = {}
            # -0.5503632372502951 for crime, -0.45764425804185416 property -- unscaled in terms of feet, with all the
            # entertainment licenses, w/o libraries, using crime-frequency-in-a-box,
            #           ultimately, with a range of -40729834.70692352 to -3271536.568326791 for community score
            # ...and actually, the correlation is the same if it's scaled to feet anyways -- with just a range of
            # -114043.53717938546 to -9160.302391315001 instead
            entry.update({"cell_community_value": x[1],
            #Oh, shit... Was this what made everything shoot up? -- was originally 0.47841092002897156 crime,
            # 0.40686143854224194 property -- unscaled in terms of feet, with all the entertainment licenses, using
            # log(property value),
            #           w/o libraries, using crime-frequency-in-a-box, ultimately, with a range of -3.0567e-07 to -2.455-08 for the community score
            #...and actually, the correlation is the same if it's scaled to feet anyways -- with just a range of -0.00010916670184906917 to -8.768581058890204e-06 instead
            #entry.update({"cell_community_value": 1.0/x[1], \
                          "cell_center_latitude": x[0][1], \
                          "cell_center_longitude": x[0][0]})
            prep.append(entry)
        ###

        ###Before, slower way of accomplishing this
        '''
        for center in coordinates:
            lat = center[1]
            long = center[0]
            for elem in repo[distancesCommunityScoreCalculations.reads[0]].find():  # Every entry in community_indicators
                d = calculate(elem['location']['coordinates'][0], elem['location']['coordinates'][1], long,lat)/gpsDifference_1000Feet#Scaling in terms of feet!
                score += d*elem['community_score']

            entry = {}
            entry.update({"cell_community_value": 1.0/score, \
                          "cell_center_latitude": lat, \
                          "cell_center_longitude": long})
            prep.append(entry)
            score=0
        #'''
        
        str_prep=', '.join(json.dumps(d) for d in prep)
        l_prep='['+str_prep+']'
        r=json.loads(l_prep)
        myrepo["boston_grid_community_values_cellSize1000sqft"].insert_many(r)
        #json.dump(entry, out)
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client =  dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(distancesCommunityScoreCalculations.contributor, distancesCommunityScoreCalculations.contributor)
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#distancesCommunityScoreCalculations',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_liquor_data, this_script)

        found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#boston_grid_community_values_cellSize1000sqft', {prov.model.PROV_LABEL:'calculating community values', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_liquor_data, endTime)
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()
        return doc

#distancesCommunityScoreCalculations.execute()
#doc = distancesCommunityScoreCalculations.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

def main():
    print("Executing: distancesCommunityScoreCalculations.py")
    distancesCommunityScoreCalculations.execute()
    doc = distancesCommunityScoreCalculations.provenance()
