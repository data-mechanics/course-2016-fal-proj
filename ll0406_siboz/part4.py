import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
import math
from matplotlib import pyplot as plt
from pprint import pprint
from sodapy import Socrata
from dataRetrieval import *
from generalHelperTemplate import *

class part4(dml.Algorithm):
    contributor = 'll0406_siboz'
    reads = ['ll0406_siboz.crimeCluster2016', 'll0406_siboz.propAssess2016', 'll0406_siboz.entertainmentLocation']
    writes = ['ll0406_siboz.entertainmentCluster', 'll0406_siboz.houseLocation_Score']
    DOMAIN = "data.cityofboston.gov"

    @staticmethod
    def execute(trail = False):
        startTime = datetime.datetime.now()
        #Setup starts here
        DOMAIN = "data.cityofboston.gov"
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ll0406_siboz', 'll0406_siboz')

        ##The retrieval code should run only once after any change to the retrieval algorithm, since it may take many time to
        ##Retrieve one huge data set

        #retrieval()

        """
        This file is the continuation of the part3.py.
        Now all the Cluster data are stored at the mongo repo, and now we will examine the relationship between those cluster data
        with transformations, very much like in proj2.py like we did with the liquor store data and the shooting crime data.
        """

        #Create pointers to all the cluster data.
        crimeCluster_2016_cursor = repo['ll0406_siboz.crimeCluster2016'].find({});
        raw2016PropAssess = repo['ll0406_siboz.propAssess2016'].find({});
        rawEntertain = repo['ll0406_siboz.entertainmentLocation'].find({});

        residentKey = ["R1","R2","R3","R4","RL","A"]

        crimeCluster_2016 = []
        Residential_2016 = []
        EntertainmentNameList = []
        EntertainmentList = []
        for item in crimeCluster_2016_cursor:
            crimeCluster_2016.append((item["coordinates"][0], item["coordinates"][1]))

        for item in raw2016PropAssess:
            temp = {k:v for k, v in item.items() if (k == "lu" or k == "living_area" or k == "av_bldg" or k == "longitude" or k == "latitude")}
            if (len(temp) == 5 and temp["lu"] in residentKey and float(temp["av_bldg"]) > 0 and float(temp["living_area"]) > 0 and temp["latitude"] != "#N/A"):
                tempTuple = (temp["lu"], float(temp["living_area"]), float(temp["av_bldg"]), float(temp["longitude"]), float(temp["latitude"]))
                Residential_2016.append(tempTuple)

        for item in rawEntertain:
            temp = {k:v for k,v in item.items() if (k == "dbaname" or k == "location")}
            #print(temp)
            if (len(temp) == 2 and temp["location"] != 'NULL'):
                if(temp["dbaname"] in EntertainmentNameList):
                    continue
                else:
                    EntertainmentNameList.append(temp["dbaname"])
                    coord = stringLocToNumLoc(temp["location"])
                    tempTuple = (temp["dbaname"], coord[1], coord[0])
                    EntertainmentList.append(tempTuple)

        entertainmentLoc = [(lon, lat) for n, lon, lat in EntertainmentList]

        entertainLocInitial = []
        """
        #Selecting 8 initial points from entertainment locations
        entertainLocInitial_temp = []
        for i in range(0, 8):
            entertainLocInitial_temp.append(entertainmentLoc[int(len(entertainmentLoc) * np.random.rand())])

        #Write to local file
        with open('localData\part4\entertainKMInitial.txt', 'w') as outfile:
            for coordinate in entertainLocInitial_temp:
                #print(coordinate)
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        """


        #Load
        with open('localData\part4\entertainKMInitial.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                entertainLocInitial.append((float(tempCoord[0]),float(tempCoord[1])))

        
        #entertainClusters = coordKMeans(entertainLocInitial, entertainmentLoc)
            

        """
        repo.dropPermanent("entertainmentCluster")
        repo.createPermanent("entertainmentCluster")
        for coord in entertainClusters:
            temp = {"type":"Point", "coordinates":[coord[0], coord[1]]}
            repo['ll0406_siboz.entertainmentCluster'].insert_one(temp)
        """

        entertainClusters = []
        entertainCursor = repo['ll0406_siboz.entertainmentCluster'].find({});

        for item in entertainCursor:
            entertainClusters.append((item["coordinates"][0], item["coordinates"][1]))

        for coord in entertainClusters:
            print("[" + str(coord[0]) + "," + str(coord[1]) + "],")

        #Here we implement a naive scoring system for the residential housing in boston area
        #The standard here is pretty simple. The overall score of a residential housing is depended
        #on the price, distance to its cloest entertaiment cluster and distance to its cloest crime location
        #And we can formulate the objective funtion here as:
        #  Z = (25  - distance to cloest entertainment cluster) * 10 - (3 + distance to cloest crime cluster) * 10 - (cost per area * 0.5)

        costLoc = [(price/area, (lon, lat)) for lu, area, price, lon, lat in Residential_2016 if price/area< 1000]
        Location_Cost_DisToCrime_DisToEntertain = []
        for house in costLoc:
            coord = house[1]
            minDisToCrime = min([geoDist(coord, crimeCluster) for crimeCluster in crimeCluster_2016])
            minDisToEntertain = min([geoDist(coord, entertainCluster) for entertainCluster in entertainClusters])
            Location_Cost_DisToCrime_DisToEntertain.append((house[1], house[0],minDisToCrime, minDisToEntertain))

        Location_Scores = [(loc, houseEval(price,distToCrime,distToEntertain)) for loc, price, distToCrime, distToEntertain in Location_Cost_DisToCrime_DisToEntertain]

        repo.dropPermanent("houseLocation_Score")
        repo.createPermanent("houseLocation_Score")
        for item in Location_Scores:
            temp = {"Location":[item[0][0], item[0][1]], "Score":item[1]}
            repo['ll0406_siboz.houseLocation_Score'].insert_one(temp)


        print(len(Location_Scores))

        """
        for d3 visualization
        

        tempList = []
        for item in Location_Scores:
            tempList.append([[item[0][0], item[0][1]], item[1]])
        tempJson = {"Loc_Score_s": tempList}

        with open('localData\part4\locScore.json', 'w') as outfile:
            json.dump(tempJson, outfile)
        """

        Scores = [score for loc, score in Location_Scores]
        avg_score = np.mean(Scores)
        std_score = np.std(Scores)

        print(avg_score)
        print(std_score)
        print(min(Scores))
        print(max(Scores))

        #The algorithm will return a list of properties that have scores that is 1.85 std above the average score.
        #Again, this is just a naive implmentation of the housing evaluation due to the limitation of avalibale datasets,
        #But in the future works, there can be many more factors to be considered.
        topScores = [score for score in Scores if score > avg_score + 1.85 * std_score]
        topScoreLocations = [(loc, score) for loc, score in Location_Scores for topScore in topScores if score == topScore]

        #print(topScoreLocations)
        with open('localData\part4\TopScoreLoc2.txt', 'w') as outfile:
            for item in topScoreLocations:
                #print(coordinate)
                coordTemp = item[0]
                coord = (coordTemp[1], coordTemp[0])
                strs = ",".join(str(x) for x in coord)
                outfile.write(strs + "\n")



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
            repo.authenticate('ll0406_siboz', 'll0406_siboz')

            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
            doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'Data_Resource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
            doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

            this_script = doc.agent('alg:ll0406_siboz#prjo1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
            crimeCluster2016 = doc.entity('dat:ll0406_siboz#crimeCluster2016', {prov.model.PROV_LABEL:'Crime Cluster 2016', prov.model.PROV_TYPE:'ont:DataSet'})
            propAssess2016 = doc.entity('dat:ll0406_siboz#propAssess2016', {prov.model.PROV_LABEL:'Property Assessment Data 2016', prov.model.PROV_TYPE:'ont:DataSet'})
            entertainmentLocation = doc.entity('dat:ll0406_siboz#entertainmentLocation', {prov.model.PROV_LABEL:'Entertainment Location', prov.model.PROV_TYPE:'ont:DataSet'})
            entertainmentCluster = doc.entity('dat:ll0406_siboz#entertainmentCluster', {prov.model.PROV_LABEL:'Entertainment Cluster', prov.model.PROV_TYPE:'ont:DataSet'})
            computeEntertainmentCluster = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            computeScore = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(computeEntertainmentCluster, this_script)
            doc.wasAssociatedWith(computeScore, this_script)

            doc.wasAttributedTo(entertainmentCluster, this_script)
            doc.wasGeneratedBy(entertainmentCluster, computeEntertainmentCluster, endTime)
            doc.wasDerivedFrom(entertainmentCluster, entertainmentLocation, computeEntertainmentCluster, computeEntertainmentCluster, computeEntertainmentCluster)

            doc.usage(computeEntertainmentCluster, entertainmentLocation, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(computeScore, crimeCluster2016, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(computeScore, propAssess2016, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(computeScore, entertainmentCluster, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )

            houseLocation_Score = doc.entity('dat:ll0406_siboz#houseLocation_Score', {prov.model.PROV_LABEL:'House Location and Score', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(houseLocation_Score, this_script)
            doc.wasGeneratedBy(houseLocation_Score, computeScore, endTime)
            doc.wasDerivedFrom(houseLocation_Score, propAssess2016, entertainmentLocation, computeScore, computeScore, computeScore)


            repo.logout()

            return doc

part4.execute()
doc = part4.provenance()
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof