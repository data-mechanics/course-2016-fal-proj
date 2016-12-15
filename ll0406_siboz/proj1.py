import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy
import math
from pprint import pprint
from sodapy import Socrata
from dataRetrieval import *
from generalHelperTemplate import *

class proj1(dml.Algorithm):
    contributor = 'll0406_siboz'
    reads = []
    writes = ["ll0406_siboz.crimeIncident", "ll0406_siboz.policeLocation", "ll0406_siboz.crimeDistToPoliceSation", "ll0406_siboz.liquorLocation", "ll0406_siboz.foodLocation", "ll0406_siboz.entertainmentLocation"]
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

        rawCrime = repo['ll0406_siboz.crimeIncident'].find({});
        rawNewCrime = repo['ll0406_siboz.newCrimeIncident'].find({});
        rawPoliceStation = repo['ll0406_siboz.policeLocation'].find({});

        interestedCrimeType = ["AGGRAVATED ASSAULT", "aggravated assault", "arrest", "Bomb", "CRIMES AGAINST CHILDREN",
                        "Drug Violation", "Firearm Violations", "HOMICIDE", "Homicide", "Robbery", "ROBBERY", "Vandalism",
                        "VANDLAISM", "WEAPONS CHARGE", "Aggravated Assault", "Explosives", "HOME INVASION"]

        ##Data Cleanup, mainly through selection
        ##Crime data
        CrimeModifiedData = []

        ##Modifiy the new Crime data (2015/8 to date) and subsitute the key "offense_code_group" as "incident_type_description"
        ##So we can merge them together as one dictionary. 
        for item in rawNewCrime:
            temp = {k:v for k,v in item.items() if (k == "year" or k == "offense_code_group" or k == "location")}
            if ((temp["location"]["coordinates"][0] != 0 and temp["location"]["coordinates"][0] != -1) and temp["offense_code_group"] in interestedCrimeType):
                temp["incident_type_description"] = temp.pop("offense_code_group")
                CrimeModifiedData.append(temp)

        for item in rawCrime:
            temp = {k:v for k,v in item.items() if (k == "year" or k == "incident_type_description" or k == "location")}
            if ((temp["location"]["coordinates"][0] != 0 and temp["location"]["coordinates"][0] != -1) and temp["incident_type_description"] in interestedCrimeType):
                CrimeModifiedData.append(temp)


        #The following random process has already been run once


        #Since the data set is still too large to for computing a k-means clutering algorithm.
        #The program will further randomly select 2000 samples from all the data points.

        #Create a random indicies array

        """
        randomInd = []
        for i in range (0, 1000):
            randomInd.append(int(len(CrimeModifiedData) * numpy.random.rand()))

        SampleCrimeDataTemp = []
        SampleCrimeData = []
        for i in range (0, 1000):
            SampleCrimeDataTemp.append(CrimeModifiedData[randomInd[i]])
        """
		


       	"""After we examined the nature of our project carefully, we think that to many factors of ranom would make it nearly
        impossible for us to make any meaningful progress later on for the optimization and statistical inference. So here we will
        sample the crime data once and store it locally, and later everytime we run the code, the random sample would always be the same
        and will produce consistent result"""
        
        #The SampleCrimeData.json has already been generated
        """
        with open('localData\sampleCrimeData.json', 'w') as outfile:
        	json.dump(SampleCrimeDataTemp, outfile)
        """

        with open('localData\sampleCrimeData.json') as data_file:
        	SampleCrimeData = json.load(data_file)


        crimeGeoList = []
        for item in SampleCrimeData:
            for k, v in item.items():
                if k == "location":
                    temp = (float(v["coordinates"][0]), float(v["coordinates"][1])) #Longitude, Latitude
                    crimeGeoList.append(temp)

        """
        After obtaining the crimeGeoList from the sample crime data, we will continue further to random sample out 9 points as the 
        initial cluster point for the k-means algorithm 
        Also for the sake of the consistency and stable output, the random selection will only run once, and the initial points are stored
        into a local json file, and later will be pushed to the database.
        """

        initialPointsTemp = []
        jsonTemp = []
        initialPoints = []


        
        #The same as above, the random process and the localData has already been generated,
        #To make sure we have consistent result, this section of code will be commented out
        """
        for i in range(0, 9):
        	initialPointsTemp.append(crimeGeoList[int(len(crimeGeoList) * numpy.random.rand())])

        with open('localData\initialKMeans.json', 'w') as outfile:
        	json.dump(initialPointsTemp, outfile)
        """
	
        with open('localData\initialKMeans.json') as data_file:
        	jsonTemp = json.load(data_file)

        for l in jsonTemp:
        	initialPoints.append((l[0],l[1]))


        crimeCluster = coordKMeans(initialPoints, crimeGeoList)


        """
        Notice that the cluster points could very likely to be less than the inital points after
        the k means algorithm. Since there could be cluster point in M that no points get to link with
        it after the re-assignment.
        """
        #Then we proceed to find what is the nearest geolocation of the police station location get an idea
        #close the station is to the cluster area.

        #Obtain the police station geolocation list.
        policeGeoList = []
        for item in rawPoliceStation:
            for k, v in item.items():
                if k == "location":
                    temp = (float(v["coordinates"][0]), float(v["coordinates"][1]))    ##Longitude, Latitude
                    policeGeoList.append(temp)

        ## Find the cloest police station to the cluster point of crime.
        CPD = [(c, p, dist(c,p)) for (c, p) in product(crimeCluster, policeGeoList)]
        CDs = [(c, dist(c,p)) for (c, p, d) in CPD]
        CD = aggregate(CDs, min)
        #print(CP)
        CP = [(c, p) for ((c,p,d),(c2,d2)) in product(CPD, CD) if c == c2 and d == d2]

        for c, p in CP:
            print(str(c[1]) + ',' + str(c[0]))
            print(str(p[1]) + ',' + str(p[0]))


        distList = []
        for item in CP:
            distList.append(geoDist(item[0],item[1]))

        distance = []
        for item in distList:
            distance.append({"distance": item})
            print({"distance": item})

        repo.dropPermanent("crimeDistToPoliceSation")
        repo.createPermanent("crimeDistToPoliceSation")
        repo['ll0406_siboz.crimeDistToPoliceSation'].insert_many(distance)
        repo['ll0406_siboz.crimeDistToPoliceSation'].find({});

        """
        repo.dropPermanent("generalCrimeCluster")
        repo.createPermanent("generalCrimeCluster")
        for coord in entertainClusters:
            temp = {"type":"Point", "coordinates":[coord[0], coord[1]]}
            repo['ll0406_siboz.entertainmentCluster'].insert_one(temp)

        """


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
            Crim_Resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:Crim_Resource', 'ont:Extension':'json'})
            PoliceLoc_Resource = doc.entity('bdp:pyxn-r3i2', {'prov:label':'Boston Police District Stations', prov.model.PROV_TYPE:'ont:PoliceLoc_Resource', 'ont:Extension':'json'})
            get_Criminal = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            get_PoliceLoc = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            compute_Dist = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(get_Criminal, this_script)
            doc.wasAssociatedWith(get_PoliceLoc, this_script)
            doc.usage(get_Criminal, Crim_Resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Retrieval',
                     'ont:Query':'?$where=location.longitude!=0'
                    }
                )
            doc.usage(get_PoliceLoc, PoliceLoc_Resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Retrieval',
                     'ont:Query':'?$select=location'
                    }
                )

            Criminal = doc.entity('dat:ll0406_siboz#Criminal', {prov.model.PROV_LABEL:'Criminal Location', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(Criminal, this_script)
            doc.wasGeneratedBy(Criminal, get_Criminal, endTime)
            doc.wasDerivedFrom(Criminal, Crim_Resource, get_Criminal, get_Criminal, get_Criminal)

            PoliceLoc = doc.entity('dat:ll0406_siboz#PoliceLoc', {prov.model.PROV_LABEL:'PoliceStation Location', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(PoliceLoc, this_script)
            doc.wasGeneratedBy(PoliceLoc, get_PoliceLoc, endTime)
            doc.wasDerivedFrom(PoliceLoc, PoliceLoc_Resource, get_PoliceLoc, get_PoliceLoc, get_PoliceLoc)

            Crim_Police_dist = doc.entity('dat:ll0406_siboz#Crim_Police_dist', {prov.model.PROV_LABEL:'Crim Police distance', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(Crim_Police_dist,this_script)
            doc.wasGeneratedBy(Crim_Police_dist, compute_Dist, endTime)
            doc.wasDerivedFrom(Crim_Police_dist, Criminal, PoliceLoc, compute_Dist, get_PoliceLoc, get_Criminal)

            repo.record(doc.serialize()) # Record the provenance document.
            repo.logout()

            return doc

proj1.execute()
doc = proj1.provenance()
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof