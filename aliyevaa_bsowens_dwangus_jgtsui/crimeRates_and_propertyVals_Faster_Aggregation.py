import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
import ast
import math
import numpy as np
from collections import defaultdict

from scipy import spatial

def calculate(x0, y0, x1, y1):
    x_d_sq = pow(abs(x0) - abs(x1), 2)
    y_d_sq = pow(abs(y0) - abs(y1), 2)
    dist = math.sqrt(x_d_sq + y_d_sq)
    return dist

#My updated code for faster crime aggregation
def findClosest(array, value, printCheck=False):
    idx = (np.absolute(array - value)).argmin()
    return array[idx]

class crimeRates(dml.Algorithm):
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'

    #Previous, with just old crime set that was incomplete
    #setExtensions = ['boston_grid_crime_rates_cellSize1000sqft']
    #titles = ['Boston Grid Cells Crime Incidence 2012 - 2015']
    #oldSetExtensions = ['crime2012_2015', 'cell_GPS_center_coordinates']

    setExtensions = ['boston_grid_crime_rates_cellSize1000sqft', 'boston_grid_properties_cellSize1000sqft']
    titles = ['Boston Grid Cells Crime Incidence 2012 - 2015', 'Property Assessment 2016']
    oldSetExtensions = ['crimes_new', 'cell_GPS_center_coordinates', 'property_assessment']

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
        # code that david would use, which read in 'cell_GPS_center_coordinates'
        cellCoordinates = repo[crimeRates_and_propertyVals_Faster_Aggregation.reads[1]]
        coordinates = []
        for cellCoord in cellCoordinates.find():
            coordinates.append((cellCoord['longitude'],cellCoord['latitude']))
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
        ###From previous method of aggregating crimes into cells
        '''
        #Asselya's code for writing crime_spots.txt
        out = open('crime_spots.txt', 'w')
        entry = dict((str(center[0]) + ' ' + str(center[1]), 0) for center in coordinates)
        #'''
        ###

        print("# Total Cell Coordinates: {}".format(len(coordinates)))
        
        # Boston GPS Map is X: Longitudes, from -71.2 to -70.95 (approximately)
        # Boston GPS Map is Y: Latitudes, from 42.2 to 42.4 (approximately)

        # David's updated code for faster crime aggregation
        cellCenterDict = {}
        for coord in coordinates:
            gpslong = coord[0]
            gpslat = coord[1]
            if gpslong in cellCenterDict:
                cellCenterDict[gpslong][gpslat] = 0
            else:
                cellCenterDict[gpslong] = {gpslat: 0}
        
        # David's updated code for faster crime aggregation
        longitudeKeys = np.array(list(cellCenterDict.keys())) # sorted(list(cellCenterDict.keys()))
        print(longitudeKeys)
        for lo in longitudeKeys:
            cellCenterDict[lo]['latitudes'] = np.array(list(cellCenterDict[lo].keys()))#sorted(list(cellCenterDict[lo].keys()))

        
        ################# Added by Asselya #################
        property_data_dict = defaultdict(list)
        prop = repo[crimeRates.reads[2]]
        count_prop = 0
        
        repo.dropPermanent(crimeRates.setExtensions[1])
        repo.createPermanent(crimeRates.setExtensions[1])
        
        for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.property_assessment.find():
            try:
                if ('coordinates' in elem['location']):
                    count_prop += 1
                    longProperty = elem['location']['coordinates'][0]
                    latProperty = elem['location']['coordinates'][1]

                    ### From Asselya's previous method of aggregating property values into cells
                    '''
                    closest_so_far = calculate(long, lat, coordinates[0][0], coordinates[0][1])
                    closest_center = str(coordinates[0][0]) + ' ' + str(coordinates[0][1])
                    for center in coordinates[1:]:
                        c_str = str(center[0]) + ' ' + str(center[1])
                        d = calculate(long, lat, center[0], center[1])
                        if d < closest_so_far:
                            closest_so_far = d
                            closest_center = c_str
                    data_dict[closest_center].append(elem['av_total'])
                    #'''
                    
                    ###David's updated code for faster property value aggregation
                    closestLong = findClosest(longitudeKeys, longProperty)
                    closestLat = findClosest(cellCenterDict[closestLong]['latitudes'], latProperty)
                    property_data_dict[str(closestLong) + ' ' + str(closestLat)].append(elem['av_total'])
                    ###
            except:
                pass

        print(count_prop)
        #print(property_data_dict['-71.123 42.2854'])
        for e in property_data_dict.keys():
            repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_properties_cellSize1000sqft.insert({str(e): property_data_dict[e]}, check_keys=False)

        print('FINISHED PROPERTIES.')
        ####################################################


        print('NOW PROCESSING CRIMES.')
        repo.dropPermanent(crimeRates.setExtensions[0])
        repo.createPermanent(crimeRates.setExtensions[0])
        crimeDataSet = repo[crimeRates.reads[0]]
        print(crimeDataSet.count())

        ###For inverse-distance of crime calculation, as discussed previously
        #'''
        listValidCrimeLocations = []
        #'''
        ###
        
        count = 0
        for elem in crimeDataSet.find():
            if ('location' in elem) and ('coordinates' in elem['location']) \
                    and (float(elem['location']['coordinates'][0]) < 0) \
                    and (float(elem['location']['coordinates'][1]) > 0):
                count += 1

                long = elem['location']['coordinates'][0]
                lat = elem['location']['coordinates'][1]
                
                ###From previous method of aggregating crimes into cells
                '''
                #Find closest cell-center to this particular crime's GPS coordinates
                closest_so_far = calculate(long, lat, coordinates[0][0], coordinates[0][1])
                closest_center = str(coordinates[0][0]) + ' ' + str(coordinates[0][1])
                for center in coordinates[1:]:
                    c_str = str(center[0]) + ' ' + str(center[1])
                    d = calculate(long, lat, center[0], center[1])
                    if d < closest_so_far:
                        closest_so_far = d
                        closest_center = c_str
                entry[closest_center] += 1
                #'''
                ###

                ###Commented-Out, for old (fast) method of just lumping frequency of crimes by cell
                #'''
                ###David's updated code for faster crime aggregation
                closestLongitude = findClosest(longitudeKeys, long)
                cellCenterDict[closestLongitude][findClosest(cellCenterDict[closestLongitude]['latitudes'], lat, True)] += 1
                
                if count % 10000 == 0:
                    print("Parsed " +  str(count) + " crime entries")
                ###
                #'''
                ###

                ###For inverse-distance of crime calculation, as discussed previously
                #'''
                listValidCrimeLocations.append((long, lat))
                #'''
                ###
        
        print("Number of valid crimes we've found: ", len(listValidCrimeLocations))

        
        ###For inverse-distance of crime calculation, as discussed previously
        '''
        #How big could this be... what, ~1700 cells by ...50,000 crimes? -- oh shit, it's like 250k crimes
        cellRows = np.array(coordinates)
        crimeCols = np.array(listValidCrimeLocations)
        Y = spatial.distance.cdist(cellRows, crimeCols, 'euclidean')
        
        #Y = 1/Y
        #inverseCellCrimeScores = [(coordinates[y], sum(Y[y])) for y in range(len(Y))]
        inverseCellCrimeScores = [(coordinates[y], sum([1/crime for crime in Y[y]])) for y in range(len(Y))]
        #'''
        ###

        
        ###From previous method of aggregating crimes into cells
        '''
        for e in entry.keys():
            #crimeDataSet.insert({str(e): entry[e]}, check_keys=False)
            myrepo[crimeRates.setExtensions[0]].insert({str(e): str(entry[e])}, check_keys=False)
        #'''
        ###

        ###Commented-Out, for old (fast) method of just lumping frequency of crimes by cell
        #'''
        # With this method, got a crime to community correlation of -0.018864163011454722
        ### David's updated code for faster crime aggregation
        for e in cellCenterDict.keys():
            curLong = cellCenterDict[e]
            curLongStr = str(e) + ' '
            for f in curLong['latitudes']:
                myrepo[crimeRates.setExtensions[0]].insert({curLongStr + str(f): str(curLong[f])}, check_keys=False)
        ###
        #'''
        ###

        ###For inverse-distance of crime calculation, as discussed previously
        '''
        for invScore in inverseCellCrimeScores:
            myrepo[crimeRates.setExtensions[0]].insert({str(invScore[0][0]) + ' ' + str(invScore[0][1]): invScore[1]}, check_keys=False)
        #'''
        ###

        
        ###Asselya's Commented-Out Code
        '''
        #Wouldn't work anymore since I changed the dictionary format of entry (now cellCenterDict)
        entry_copy = str(entry)
        entry_copy = entry_copy.replace(" '-", ' {lat:-')
        entry_copy = entry_copy.replace(" 42", ', lng: 42')
        entry_copy = entry_copy.replace("': ", ', count: ')
        entry_copy = entry_copy.replace(", {", '}, {')
        entry_copy = "{lat:" + entry_copy[4:]
        entry_copy = entry_copy.replace("}, ", "},")
        # json.dump(entry_copy, out)
        out.write(entry_copy)
        #'''
        ###


        
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


#crimeRates.execute()
#doc = crimeRates.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))

def main():
    print("Executing: crimeRates_and_propertyVals_Faster_Aggregation.py")
    crimeRates.execute()
    doc = crimeRates.provenance()
