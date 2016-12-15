import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy as np
import math
from pprint import pprint
from sodapy import Socrata
from dataRetrieval import *
from generalHelperTemplate import *

class part3(dml.Algorithm):
    contributor = 'll0406_siboz'
    reads = ['ll0406_siboz.crimeIncident','ll0406_siboz.newCrimeIncident', 'll0406_siboz.propAssess2014','ll0406_siboz.propAssess2015','ll0406_siboz.propAssess2016']
    writes = ["ll0406_siboz.crimeCluster2014", "ll0406_siboz.lowValuePropCluster2014", "ll0406_siboz.crimeCluster2015", "ll0406_siboz.lowValuePropCluster2015", 
                "ll0406_siboz.crimeCluster2016", "ll0406_siboz.lowValuePropCluster2016"]
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

        retrieval()

        """
        After assessing the result we got from examing how the change of the liquor store location effect the change of the crime cluster, we want to reconsider
        other factors and compute its likelihood to effect the change of the crime cluster from 2014 to 2016

        
        At this section, we will examine how the property value change from 2014 to 2016 affect the crime cluster change from 2014 to 2016.
        Since the k-means calculation may take too quiet some time, this part designated to calculate the cluster of low value property and 
        crime through 2014-2016. All the cluster data are pushed to mongo repo.

        """

        #Crime pointers
        rawCrime = repo['ll0406_siboz.crimeIncident'].find({});
        rawNewCrime = repo['ll0406_siboz.newCrimeIncident'].find({});
        raw2014PropAssess = repo['ll0406_siboz.propAssess2014'].find({});
        raw2015PropAssess = repo['ll0406_siboz.propAssess2015'].find({});
        raw2016PropAssess = repo['ll0406_siboz.propAssess2016'].find({});

        interestedCrimeType = ["AGGRAVATED ASSAULT", "aggravated assault", "arrest", "Bomb", "CRIMES AGAINST CHILDREN",
                        "Drug Violation", "Firearm Violations", "HOMICIDE", "Homicide", "Robbery", "ROBBERY", "Vandalism",
                        "VANDLAISM", "WEAPONS CHARGE", "Aggravated Assault", "Explosives", "HOME INVASION"]

        ##Crime data
        CrimeModifiedData = []

        ##Modifiy the new Crime data (2015/8 to date) and subsitute the key "offense_code_group" as "incident_type_description"
        ##So we can merge them together as one dictionary. 
        ##Also we will transform the list of dictionaries to list of tuples, which makes transformation eaiser.
        ##The Tuple will be in the form of (incident type, year of crime, location longitude, location latitude)
        for item in rawNewCrime:
            temp = {k:v for k,v in item.items() if (k == "year" or k == "offense_code_group" or k == "location")}
            if ((temp["location"]["coordinates"][0] != 0 and temp["location"]["coordinates"][0] != -1) and temp["offense_code_group"] in interestedCrimeType):
                temp["incident_type_description"] = temp.pop("offense_code_group")
                tempTuple = (temp["incident_type_description"], int(temp["year"]), temp["location"]["coordinates"][0], temp["location"]["coordinates"][1])
                CrimeModifiedData.append(tempTuple)

        for item in rawCrime:
            temp = {k:v for k,v in item.items() if (k == "year" or k == "incident_type_description" or k == "location")}
            if ((temp["location"]["coordinates"][0] != 0 and temp["location"]["coordinates"][0] != -1) and temp["incident_type_description"] in interestedCrimeType):
                tempTuple = (temp["incident_type_description"], int(temp["year"]), temp["location"]["coordinates"][0], temp["location"]["coordinates"][1])
                CrimeModifiedData.append(tempTuple)


        #For the purpose of this feature, we only need the geolocation of crimes of different years, following code will obtain the geolocations of crimes of different year
        #Through selection.
        CrimeGeoLoc_2014 = [(lon, lati) for t, y, lon, lati in CrimeModifiedData if y == 2014]
        CrimeGeoLoc_2015 = [(lon, lati) for t, y, lon, lati in CrimeModifiedData if y == 2015]
        CrimeGeoLoc_2016 = [(lon, lati) for t, y, lon, lati in CrimeModifiedData if y == 2016]

        print(len(CrimeGeoLoc_2014))
        print(len(CrimeGeoLoc_2015))
        print(len(CrimeGeoLoc_2016))


        ##For now, we will leave the crime locations here and do the random sampling later, we will deal with the property assessment data
        ##Also we are only examining the residential buildings here
        ##All the tuples in the Residential_2014 are in the form of (Land use, living area, assessed value for the building. longitude, latitude)

        residentKey = ["R1","R2","R3","R4","RL","A"]
        Residential_2014 = []
        Residential_2015 = []
        Residential_2016 = []

        pprint(raw2016PropAssess[0])

        for item in raw2014PropAssess:
            temp = {k:v for k,v in item.items() if (k == "lu" or k == "living_area" or k == "av_bldg" or k == "location")}
            if (len(temp) == 4 and temp["lu"] in residentKey and float(temp["av_bldg"]) > 0 and float(temp["living_area"]) > 0 and len(temp["location"]) > 8):
                coordinate = stringLocToNumLoc(temp["location"]) #coordinate is in the form of latitude, longitude
                tempTuple = (temp["lu"], float(temp["living_area"]), float(temp["av_bldg"]), coordinate[1], coordinate[0])
                Residential_2014.append(tempTuple)

        #Here we define the costPerArea as value for the building divided by living area
        costPerArea_Loc_2014 = [(value/area, longi, lati) for lu, area, value, longi, lati in Residential_2014 if value/area < 1000] #The last condiiton here is to eliminate some outliers
        #print(costPerArea_Loc_2014)
        costPerAreaList_2014 = [cost for cost, longi, lati in costPerArea_Loc_2014]


        avgCost_2014 = np.mean(costPerAreaList_2014)
        costStd_2014 = np.std(costPerAreaList_2014)
        medianCost_2014 = np.median(costPerAreaList_2014)


        ##Consider the upper bound for the low-value property as average cost per value - 1.1 * standard devation of the cost per value list
        ##Choosing the number of 1.1
        ##
        ##The reason for choosing 1.1 std is based on experimenting with the database, we found that the base 1.1 
        ##
        lowValueUpperBound_2014 = avgCost_2014 - 1.1 * costStd_2014
        ##Get a list of low value property location for year of 2014
        lowValuePropLocation_2014 = [(longi, lati) for cost, longi, lati in costPerArea_Loc_2014 if cost < lowValueUpperBound_2014]

        ##Repeat the process for 2015 and 2016.
        ##2015
        for item in raw2015PropAssess:
            temp = {k:v for k, v in item.items() if (k == "lu" or k == "living_area" or k == "av_bldg" or k == "location")}
            #pprint(temp)
            if (len(temp) == 4 and temp["lu"] in residentKey and float(temp["av_bldg"]) > 0 and float(temp["living_area"]) > 0 and temp["location"][1] != "0"):
                coordinate = stringLocToNumLoc(temp["location"])
                tempTuple = (temp["lu"], float(temp["living_area"]), float(temp["av_bldg"]), coordinate[1], coordinate[0])
                Residential_2015.append(tempTuple)

        costPerArea_Loc_2015 = [(value/area, longi, lati) for lu, area, value, longi, lati in Residential_2015 if value/area < 1000] #The last condiiton here is to eliminate some outliers
        costPerAreaList_2015 = [cost for cost, longi, lati in costPerArea_Loc_2015]

        avgCost_2015 = np.mean(costPerAreaList_2015)
        costStd_2015 = np.std(costPerAreaList_2015)
        medianCost_2015 = np.median(costPerAreaList_2015)

        lowValueUpperBound_2015 = avgCost_2015 - 1.1 * costStd_2015
        lowValuePropLocation_2015 = [(longi, lati) for cost, longi, lati in costPerArea_Loc_2015 if cost < lowValueUpperBound_2015]

        ##2016
        for item in raw2016PropAssess:
            temp = {k:v for k, v in item.items() if (k == "lu" or k == "living_area" or k == "av_bldg" or k == "longitude" or k == "latitude")}
            if (len(temp) == 5 and temp["lu"] in residentKey and float(temp["av_bldg"]) > 0 and float(temp["living_area"]) > 0 and temp["latitude"] != "#N/A"):
                tempTuple = (temp["lu"], float(temp["living_area"]), float(temp["av_bldg"]), float(temp["longitude"]), float(temp["latitude"]))
                Residential_2016.append(tempTuple)

        costPerArea_Loc_2016 = [(value/area, longi, lati) for lu, area, value, longi, lati in Residential_2016 if value/area < 1000] #The last condiiton here is to eliminate some outliers
        costPerAreaList_2016 = [cost for cost, longi, lati in costPerArea_Loc_2016]

        avgCost_2016 = np.mean(costPerAreaList_2016)
        costStd_2016 = np.std(costPerAreaList_2016)
        medianCost_2016 = np.median(costPerAreaList_2016)

        lowValueUpperBound_2016 = avgCost_2016 - 1.1 * costStd_2016
        lowValuePropLocation_2016 = [(longi, lati) for cost, longi, lati in costPerArea_Loc_2016 if cost < lowValueUpperBound_2016]



        ##After obtaining the low value property location lists for 2014 - 2016 and interested crime locations lists for 2014 - 2016, we proceed to next step to
        ##find the clusters using K-means algorithm. And for the sake of the consistent output, we will only run the random selecting process once and store them
        ##To a local file.

        ##Compute crime clusters 2014-2016
        
        """
        #Random Selection Process
        sampleCrimeLoc_2014_temp = []
        sampleCrimeLoc_2015_temp = []
        sampleCrimeLoc_2016_temp = []
        crimeLoc_KMInitial_2014 = [] #Intial points for k-means
        crimeLoc_KMInitial_2015 = []
        crimeLoc_KMInitial_2016 = []
        for i in range(0, 1500):
            sampleCrimeLoc_2014_temp.append(CrimeGeoLoc_2014[int(len(CrimeGeoLoc_2014) * np.random.rand())])
            sampleCrimeLoc_2015_temp.append(CrimeGeoLoc_2015[int(len(CrimeGeoLoc_2015) * np.random.rand())])
            sampleCrimeLoc_2016_temp.append(CrimeGeoLoc_2016[int(len(CrimeGeoLoc_2016) * np.random.rand())])

        for i in range(0, 8):
            crimeLoc_KMInitial_2014.append(sampleCrimeLoc_2014_temp[int(len(sampleCrimeLoc_2014_temp) * np.random.rand())])
            crimeLoc_KMInitial_2015.append(sampleCrimeLoc_2015_temp[int(len(sampleCrimeLoc_2015_temp) * np.random.rand())])
            crimeLoc_KMInitial_2016.append(sampleCrimeLoc_2016_temp[int(len(sampleCrimeLoc_2016_temp) * np.random.rand())])

        #Store them locally, will be pushed to database later
        with open('localData\part3\sampleCrime_2014.txt', 'w') as outfile:
            for coordinate in sampleCrimeLoc_2014_temp:
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        with open('localData\part3\sampleCrime_2015.txt', 'w') as outfile:
            for coordinate in sampleCrimeLoc_2015_temp:
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        with open('localData\part3\sampleCrime_2016.txt', 'w') as outfile:
            for coordinate in sampleCrimeLoc_2016_temp:
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        with open('localData\part3\crime_KMInitial_2014.txt', 'w') as outfile:
            for coordinate in crimeLoc_KMInitial_2014:
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        with open('localData\part3\crime_KMInitial_2015.txt', 'w') as outfile:
            for coordinate in crimeLoc_KMInitial_2015:
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        with open('localData\part3\crime_KMInitial_2016.txt', 'w') as outfile:
            for coordinate in crimeLoc_KMInitial_2016:
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        
        """
        sampleCrimeLoc_2014 = []
        sampleCrimeLoc_2015 = []
        sampleCrimeLoc_2016 = []
        sampleCrimeKMInitial_2014 = []
        sampleCrimeKMInitial_2015 = []
        sampleCrimeKMInitial_2016 = []
        #Read coordinates
        with open('localData\part3\sampleCrime_2014.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                sampleCrimeLoc_2014.append((float(tempCoord[0]),float(tempCoord[1])))
        with open('localData\part3\sampleCrime_2015.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                sampleCrimeLoc_2015.append((float(tempCoord[0]),float(tempCoord[1])))
        with open('localData\part3\sampleCrime_2016.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                sampleCrimeLoc_2016.append((float(tempCoord[0]),float(tempCoord[1])))
        with open('localData\part3\crime_KMInitial_2014.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                sampleCrimeKMInitial_2014.append((float(tempCoord[0]),float(tempCoord[1])))
        with open('localData\part3\crime_KMInitial_2015.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                sampleCrimeKMInitial_2015.append((float(tempCoord[0]),float(tempCoord[1])))
        with open('localData\part3\crime_KMInitial_2016.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                sampleCrimeKMInitial_2016.append((float(tempCoord[0]),float(tempCoord[1])))

        #print(sampleCrimeKMInitial_2016)

        
        crimeCluster_2014 = coordKMeans(sampleCrimeKMInitial_2014, sampleCrimeLoc_2014)
        crimeCluster_2015 = coordKMeans(sampleCrimeKMInitial_2015, sampleCrimeLoc_2015)
        crimeCluster_2016 = coordKMeans(sampleCrimeKMInitial_2016, sampleCrimeLoc_2016)

        ##Calculate the Low-value property cluster. Since the size of low-value property is not too huge,
        ##we will use all of them but the data of 2016 is too large can it cause the memory error, so the
        ##data of 2016 will still be sampled,  and we need sample initial points.


        ##sampleLowValuePropLoc_2016_temp = []
        sampleLowValuePropLoc_2016 = []

        """
        for i in range(0, 1500):
            sampleLowValuePropLoc_2016_temp.append(lowValuePropLocation_2016[int(len(lowValuePropLocation_2016) * np.random.rand())])

        with open('localData\part3\sampleLowValuePropLoc_2016.txt', 'w') as outfile:
            for coordinate in sampleLowValuePropLoc_2016_temp:
                #print(coordinate)
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        """

        with open('localData\part3\sampleLowValuePropLoc_2016.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                sampleLowValuePropLoc_2016.append((float(tempCoord[0]),float(tempCoord[1])))



        #lowValuePropKMInitial_2014_temp = []
        #lowValuePropKMInitial_2015_temp = []
        #lowValuePropKMInitial_2016_temp = []
        lowValuePropKMInitial_2014 = []
        lowValuePropKMInitial_2015 = []
        lowValuePropKMInitial_2016 = []

        """
        for i in range(0, 8):
            lowValuePropKMInitial_2014_temp.append(lowValuePropLocation_2014[int(len(lowValuePropLocation_2014) * np.random.rand())])
            lowValuePropKMInitial_2015_temp.append(lowValuePropLocation_2015[int(len(lowValuePropLocation_2015) * np.random.rand())])
            lowValuePropKMInitial_2016_temp.append(sampleLowValuePropLoc_2016[int(len(sampleLowValuePropLoc_2016) * np.random.rand())])
        #print(lowValuePropKMInitial_2014_temp)

        
        #Writing to local files.
        with open('localData\part3\lowValuePropKMInitial_2014.txt', 'w') as outfile:
            for coordinate in lowValuePropKMInitial_2014_temp:
                #print(coordinate)
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        with open('localData\part3\lowValuePropKMInitial_2015.txt', 'w') as outfile:
            for coordinate in lowValuePropKMInitial_2015_temp:
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        
        with open('localData\part3\lowValuePropKMInitial_2016.txt', 'w') as outfile:
            for coordinate in lowValuePropKMInitial_2016_temp:
                strs = ",".join(str(x) for x in coordinate)
                outfile.write(strs + "\n")
        """

        #Reading from files
        with open('localData\part3\lowValuePropKMInitial_2014.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                lowValuePropKMInitial_2014.append((float(tempCoord[0]),float(tempCoord[1])))
        with open('localData\part3\lowValuePropKMInitial_2015.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                lowValuePropKMInitial_2015.append((float(tempCoord[0]),float(tempCoord[1])))
        with open('localData\part3\lowValuePropKMInitial_2016.txt') as data_file:
            for line in data_file:
                tempCoord = line.rstrip('\n').split(',')
                lowValuePropKMInitial_2016.append((float(tempCoord[0]),float(tempCoord[1])))

        lowValueCluster_2014 = coordKMeans(lowValuePropKMInitial_2014, lowValuePropLocation_2014)
        lowValueCluster_2015 = coordKMeans(lowValuePropKMInitial_2015, lowValuePropLocation_2015)
        lowValueCluster_2016 = coordKMeans(lowValuePropKMInitial_2016, sampleLowValuePropLoc_2016)

        print("## Crime Clusters")
        print("## 2014 Cluster")
        print(crimeCluster_2014)
        print("## 2015 Cluster")
        print(crimeCluster_2015)
        print("## 2016 Cluster")
        print(crimeCluster_2016)
        
        print("## Low Value Properties Clusters")
        print("## Low Value Cluster 2014")
        print(lowValueCluster_2014)
        print("## Low Value Cluster 2015")
        print(lowValueCluster_2015)
        print("## Low Value Cluster 2016")
        print(lowValueCluster_2016)

        """
        Since the K-means algorithm may take  siginificant amount of time with (around 15 min),
        so here we will store all the k-means results to the mongo repo.(Since we are using the same sample and the same initial points.
        the results of each run should always be the same)
        """

        repo.dropPermanent("crimeCluster2014")
        repo.createPermanent("crimeCluster2014")
        for coord in crimeCluster_2014:
            temp = {"type":"Point", "coordinates":[coord[0], coord[1]]}
            repo['ll0406_siboz.crimeCluster2014'].insert_one(temp)

        repo.dropPermanent("crimeCluster2015")
        repo.createPermanent("crimeCluster2015")
        for coord in crimeCluster_2015:
            temp = {"type":"Point", "coordinates":[coord[0], coord[1]]}
            repo['ll0406_siboz.crimeCluster2015'].insert_one(temp)

        repo.dropPermanent("crimeCluster2016")
        repo.createPermanent("crimeCluster2016")
        for coord in crimeCluster_2016:
            temp = {"type":"Point", "coordinates":[coord[0], coord[1]]}
            repo['ll0406_siboz.crimeCluster2016'].insert_one(temp)

        repo.dropPermanent("lowValuePropCluster2014")
        repo.createPermanent("lowValuePropCluster2014")
        for coord in lowValueCluster_2014:
            temp = {"type":"Point", "coordinates":[coord[0], coord[1]]}
            repo['ll0406_siboz.lowValuePropCluster2014'].insert_one(temp)

        repo.dropPermanent("lowValuePropCluster2015")
        repo.createPermanent("lowValuePropCluster2015")
        for coord in lowValueCluster_2015:
            temp = {"type":"Point", "coordinates":[coord[0], coord[1]]}
            repo['ll0406_siboz.lowValuePropCluster2015'].insert_one(temp)

        repo.dropPermanent("lowValuePropCluster2016")
        repo.createPermanent("lowValuePropCluster2016")
        for coord in lowValueCluster_2016:
            temp = {"type":"Point", "coordinates":[coord[0], coord[1]]}
            repo['ll0406_siboz.lowValuePropCluster2016'].insert_one(temp)



        print("start time is: " + str(startTime))
        print("end time is: " + str(datetime.datetime.now()))


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

            this_script = doc.agent('alg:ll0406_siboz#part3', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
            crimeIncident = doc.entity('dat:ll0406_siboz#crimeIncident', {prov.model.PROV_LABEL:'2012-2015 Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
            newCrimeIncident = doc.entity('dat:ll0406_siboz#newCrimeIncident', {prov.model.PROV_LABEL:'2015-2016 Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
            propAssess2014 = doc.entity('dat:ll0406_siboz#propAssess2014', {prov.model.PROV_LABEL:'Property Assessment Data 2014', prov.model.PROV_TYPE:'ont:DataSet'})
            propAssess2015 = doc.entity('dat:ll0406_siboz#propAssess2015', {prov.model.PROV_LABEL:'Property Assessment Data 2015', prov.model.PROV_TYPE:'ont:DataSet'})
            propAssess2016 = doc.entity('dat:ll0406_siboz#propAssess2016', {prov.model.PROV_LABEL:'Property Assessment Data 2016', prov.model.PROV_TYPE:'ont:DataSet'})

            computeCrimeCluster2014 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            computeCrimeCluster2015 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            computeCrimeCluster2016 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            computeLowValuePropertyCluster2014 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            computeLowValuePropertyCluster2015 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            computeLowValuePropertyCluster2016 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(computeCrimeCluster2014, this_script)
            doc.wasAssociatedWith(computeCrimeCluster2015, this_script)
            doc.wasAssociatedWith(computeCrimeCluster2016, this_script)
            doc.wasAssociatedWith(computeLowValuePropertyCluster2014, this_script)
            doc.wasAssociatedWith(computeLowValuePropertyCluster2015, this_script)
            doc.wasAssociatedWith(computeLowValuePropertyCluster2016, this_script)

            doc.usage(computeCrimeCluster2014, crimeIncident, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(computeCrimeCluster2015, crimeIncident, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(computeCrimeCluster2016, newCrimeIncident, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(computeLowValuePropertyCluster2014, propAssess2014, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(computeLowValuePropertyCluster2015, propAssess2015, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(computeLowValuePropertyCluster2016, propAssess2016, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )

            crimeCluster2014 = doc.entity('dat:ll0406_siboz#crimeCluster2014', {prov.model.PROV_LABEL:'Crime Cluster 2014', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(crimeCluster2014, this_script)
            doc.wasGeneratedBy(crimeCluster2014, computeCrimeCluster2014, endTime)
            doc.wasDerivedFrom(crimeCluster2014, crimeIncident, computeCrimeCluster2014, computeCrimeCluster2014, computeCrimeCluster2014)

            crimeCluster2015 = doc.entity('dat:ll0406_siboz#crimeCluster2015', {prov.model.PROV_LABEL:'Crime Cluster 2015', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(crimeCluster2015, this_script)
            doc.wasGeneratedBy(crimeCluster2015, computeCrimeCluster2015, endTime)
            doc.wasDerivedFrom(crimeCluster2015, crimeIncident, computeCrimeCluster2015, computeCrimeCluster2015, computeCrimeCluster2015)

            crimeCluster2016 = doc.entity('dat:ll0406_siboz#crimeCluster2016', {prov.model.PROV_LABEL:'Crime Cluster 2016', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(crimeCluster2016, this_script)
            doc.wasGeneratedBy(crimeCluster2016, computeCrimeCluster2016, endTime)
            doc.wasDerivedFrom(crimeCluster2016, newCrimeIncident, computeCrimeCluster2016, computeCrimeCluster2016, computeCrimeCluster2016)

            lowValuePropCluster2014 = doc.entity('dat:ll0406_siboz#lowValuePropCluster2014', {prov.model.PROV_LABEL:'Low Value Properties Cluster 2014', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(lowValuePropCluster2014, this_script)
            doc.wasGeneratedBy(lowValuePropCluster2014, computeLowValuePropertyCluster2014, endTime)
            doc.wasDerivedFrom(lowValuePropCluster2014, propAssess2014, computeLowValuePropertyCluster2014, computeLowValuePropertyCluster2014, computeLowValuePropertyCluster2014)

            lowValuePropCluster2015 = doc.entity('dat:ll0406_siboz#lowValuePropCluster2015', {prov.model.PROV_LABEL:'Low Value Properties Cluster 2015', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(lowValuePropCluster2015, this_script)
            doc.wasGeneratedBy(lowValuePropCluster2015, computeLowValuePropertyCluster2015, endTime)
            doc.wasDerivedFrom(lowValuePropCluster2015, propAssess2015, computeLowValuePropertyCluster2015, computeLowValuePropertyCluster2015, computeLowValuePropertyCluster2015)

            lowValuePropCluster2016 = doc.entity('dat:ll0406_siboz#lowValuePropCluster2016', {prov.model.PROV_LABEL:'Low Value Properties Cluster 2016', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(lowValuePropCluster2016, this_script)
            doc.wasGeneratedBy(lowValuePropCluster2016, computeLowValuePropertyCluster2016, endTime)
            doc.wasDerivedFrom(lowValuePropCluster2016, propAssess2016, computeLowValuePropertyCluster2016, computeLowValuePropertyCluster2016, computeLowValuePropertyCluster2016)

            repo.record(doc.serialize()) # Record the provenance document.
            repo.logout()

            return doc

part3.execute()
doc = part3.provenance()
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof