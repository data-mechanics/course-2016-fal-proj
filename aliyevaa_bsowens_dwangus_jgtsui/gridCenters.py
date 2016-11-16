import json
import dml
import prov.model
import datetime
import uuid
import time
import ast
from geopy.distance import vincenty as vct
from bson.code import Code

import shapefile
import pyproj
from pyproj import transform
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.path as mplPath


class gridCenters(dml.Algorithm):
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
    
    titles = ['Boston Grid Cell GPS Centers (1000-FT Cells)']
    setExtensions = ['cell_GPS_center_coordinates']

    reads = []
    writes = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in setExtensions]

    dataSetDict = {}
    for i in range(len(setExtensions)):
        dataSetDict[setExtensions[i]] = (writes[i], titles[i])

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        print("Starting execution of script @{}".format(startTime))
        start = time.time()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(gridCenters.contributor, gridCenters.contributor)
        myrepo = repo.aliyevaa_bsowens_dwangus_jgtsui
        
        for newDataSet in gridCenters.setExtensions:#Only runs once, so idk why we're doing a for-loop... oh well.
            newName = newDataSet
            reference = myrepo[newName]
            repo.dropPermanent(newName)
            repo.createPermanent(newName)

            bostonShapeFile = shapefile.Reader("bos_land.shp")

            bostonShapeList = bostonShapeFile.shapes()
            bostonShape = bostonShapeList[0]
            points = bostonShape.points

            bostonPRJ = pyproj.Proj("+proj=lcc +lat_1=41.71666666666667 +lat_2=42.68333333333333 +lat_0=41 +lon_0=-71.5 +x_0=200000 +y_0=750000.0000000001 +datum=NAD83 +units=us-ft +no_defs",preserve_units= True)
            wgs = pyproj.Proj("+init=EPSG:4326")
            GPSpoints = [pyproj.transform(bostonPRJ, wgs, tup[0], tup[1]) for tup in points]

            partsX = [[]]
            partsY = [[]]
            for i in range(len(GPSpoints)):
                pX = partsX[-1]
                pY = partsY[-1]
                curPointTup = GPSpoints[i]
                pX.append(curPointTup[0])
                pY.append(curPointTup[1])
                if len(pX) == 1:
                    continue
                elif (pX[0], pY[0]) == curPointTup:
                    partsX.append([])
                    partsY.append([])
                
            #There are main sub-divisions of all 143 divisions of Boston's shapefile;
            #only really focusing on the mainland and airport/East Boston chunks
            validParts = [0,114]
            mainland = [(partsX[0][i], partsY[0][i]) for i in range(len(partsX[0]))]
            airport = [(partsX[114][i], partsY[114][i]) for i in range(len(partsX[114]))]

            path1 = mplPath.Path(np.array(mainland))
            path2 = mplPath.Path(np.array(airport))
            cellCenters = []

            #Enter in a number of square feet
            def resolution(numFeet):
                gpsDifference_1000Feet = 0.0028
                return (numFeet/1000.0)*0.0028
            
            squareHeight = resolution(1000)
            sHalf = squareHeight/2.0
            xStart = -71.20 + sHalf
            xEnd = -70.95 - sHalf
            xRange = xEnd - xStart
            yStart = 42.20 + sHalf
            yEnd = 42.40 - sHalf
            yRange = yEnd - yStart

            widthPartition = int(xRange/squareHeight)
            heightPartition = int(yRange/squareHeight)
            #1000-square-feet --> 88 cells wide, 70 cells high, Num-Cells: 6160
            print("Wide: {}, High: {}, Num-Cells: {}".format(widthPartition, heightPartition, widthPartition*heightPartition))
            begin = time.time()
            for i in range(widthPartition):
                xCoord = xStart + i*squareHeight
                for j in range(heightPartition):
                    yCoord = yStart + j*squareHeight
                    curCoord = (xCoord,yCoord)
                    if path1.contains_point(curCoord) or path2.contains_point(curCoord):
                        cellCenters.append(curCoord)
            #Number of Valid Cells: 1728; took 65.2279999256 seconds.
            print("Number of Valid Cells: {}; took {} seconds.".format(len(cellCenters), time.time() - begin))

            for c in cellCenters:
                reference.insert({'latitude': c[1], 'longitude':c[0]})

        repo.logout()

        endTime = datetime.datetime.now()
        print("\nStorage of new dataset {} took:\n{} seconds\n...and ended at:\n{}\n".format(gridCenters.titles[0], time.time() - start, endTime))
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
        repo.authenticate(gridCenters.contributor, gridCenters.contributor)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#gridCenters', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        for key in gridCenters.dataSetDict.keys():
            #How to say that this dataset was generated from multiple sources?
            get_something = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(get_something, this_script)
            something = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#' + key, {prov.model.PROV_LABEL:gridCenters.dataSetDict[key][1], prov.model.PROV_TYPE: 'ont:DataSet'})
            doc.wasAttributedTo(something, this_script)
            doc.wasGeneratedBy(something, get_something, endTime)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

gridCenters.execute()
doc = gridCenters.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
