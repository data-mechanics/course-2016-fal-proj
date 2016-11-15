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

class proj2(dml.Algorithm):
    contributor = 'll0406_siboz'
    reads = []
    writes = ["ll0406_siboz.crimeIncident", "ll0406_siboz.policeLocation", "ll0406_siboz.crimeDistToPoliceSation", \
    "ll0406_siboz.liquorLocation", "ll0406_siboz.foodLocation", "ll0406_siboz.entertainmentLocation", "shootingCrimeIncident"]
    DOMAIN = "data.cityofboston.gov"

    @staticmethod
    def execute(trail = True):
        startTime = datetime.datetime.now()
        #Setup starts here
        DOMAIN = "data.cityofboston.gov"
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ll0406_siboz', 'll0406_siboz')

        retrieval()

        rawCrime = repo['ll0406_siboz.crimeIncident'].find({});
        rawLiquor = repo['ll0406_siboz.liquorLocation'].find({});
        rawEntertain = repo['ll0406_siboz.entertainmentLocation'].find({});
        rawShooting  = repo['ll0406_siboz.shootingCrimeIncident'].find({});

        liquorModifiedTemp = []
        liquorModified = []

        
        ct = 0
        for item in rawLiquor:
            ct+=1
            temp = {k:v for k,v in item.items() if (k == "issdttm" or k=="location")} 
            liquorModifiedTemp.append(temp)

        print(ct)

        for item in liquorModifiedTemp:
            #print(item["location"])
            if (float(item["location"]["coordinates"][0]) != 0.0):
                temp = {"year": int(item["issdttm"][0:10].split("-")[0])}
                item.update(temp)
                liquorModified.append(item)
        



        #Then we proceed to find all the locations of the liquor stores opened in 2013
        liquorPartial_1 = []
        liquorPartial_2 = []
        for item in liquorModified:
            if item["year"] == 2013:
                liquorPartial_1.append(item)


        ## 2013-2016
        for item in liquorModified:
            if item["year"] >= 2014: 
                liquorPartial_2.append(item)
        liquorPartial_2.extend(liquorPartial_1)

        print(len(liquorPartial_2))

        liquorPartial_1_GeoList = []
        for item in liquorPartial_1:
            for k, v in item.items():
                if k == "location":
                    temp = (float(v["coordinates"][0]), float(v["coordinates"][1])) #Longitude, Latitude
                    liquorPartial_1_GeoList.append(temp)

        liquorPartial_2_GeoList = []
        for item in liquorPartial_2:
            for k, v in item.items():
                if k == "location":
                    temp = (float(v["coordinates"][0]), float(v["coordinates"][1])) #Longitude, Latitude
                    liquorPartial_2_GeoList.append(temp)

        
        liquorInitial_1_temp = []
        liquorInitial_2_temp = []
        liquorPartial_1_initial = []
        liquorPartial_2_initial = []

        """
        Randomly generate 8 starting initial cluster point from liquor locations
        
        for i in range(0, 8):
            liquorInitial_1_temp.append(liquorPartial_1_GeoList[int(len(liquorPartial_1_GeoList) * numpy.random.rand())])
            liquorInitial_2_temp.append(liquorPartial_2_GeoList[int(len(liquorPartial_2_GeoList) * numpy.random.rand())])

        with open('localData\proj2\liquorPartial_1_initial.json', 'w') as outfile:
            json.dump(liquorInitial_1_temp, outfile)
        with open('localData\proj2\liquorPartial_2_initial.json', 'w') as outfile:
            json.dump(liquorInitial_2_temp, outfile)
        
        """
        liquorInitial_1_temp = []
        liquorInitial_2_temp = []
        with open('localData\proj2\liquorPartial_1_initial.json') as data_file:
            liquorInitial_1_temp = json.load(data_file)
        with open('localData\proj2\liquorPartial_2_initial.json') as data_file:
            liquorInitial_2_temp = json.load(data_file)

        for l in liquorInitial_1_temp:
            liquorPartial_1_initial.append((l[0],l[1]))
        for l in liquorInitial_2_temp:
            liquorPartial_2_initial.append((l[0],l[1]))


        liquorCluster1 = coordKMeans(liquorPartial_1_initial, liquorPartial_1_GeoList)
        liquorCluster2 = coordKMeans(liquorPartial_2_initial, liquorPartial_2_GeoList)


        ## Find the cloest matched crime cluster point for the old cluster point
        L1L2D = [(l1, l2, geoDist(l1,l2)) for (l1, l2) in product(liquorCluster1, liquorCluster2)]
        L1Ds = [(l1, geoDist(l1,l2)) for (l1,l2,d) in L1L2D]
        L1D = aggregate(L1Ds, min)
        #print(CP)
        L1L2 = [(l1, l2) for ((l1,l2,d),(l1_2,d_2)) in product(L1L2D, L1D) if l1 == l1_2 and d == d_2]

        ##Calculate the Difference Vector between two 
        LiquorDiffVector = []
        for item in L1L2:
            LiquorDiffVector.extend([[(item[0]),(100 * (item[1][0] -item[0][0]), 1000 * (item[1][1] -item[0][1]))]])



        #for item in CrimeModifiedData:
        ##Crime data
        shootingCrimeModifiedTemp = []
        shootingCrimeModified = []
        for item in rawShooting:
            temp = {k:v for k,v in item.items() if (k == "month" or k=="year" or k == "location")} 
            shootingCrimeModifiedTemp.append(temp)

        for item in shootingCrimeModifiedTemp:
            if (float(item["location"]["coordinates"][0]) != 0.0):
                shootingCrimeModified.append(item)

        ##Here we are gathering all shooting crime location that happened between:
        ##  2013  
        shootingPartial_1 = []
        shootingPartial_2 = []
        for item in shootingCrimeModified:
            if item["year"] == "2013":
                shootingPartial_1.append(item)


        ## 2013-2016
        for item in shootingCrimeModified:
            if item["year"] == "2015" or item["year"] == "2016" or item["year"] == "2014": 
                shootingPartial_2.append(item)
        shootingPartial_2.extend(shootingPartial_1)


        shootingPartial_1_GeoList = []
        for item in shootingPartial_1:
            for k, v in item.items():
                if k == "location":
                    temp = (float(v["coordinates"][0]), float(v["coordinates"][1])) #Longitude, Latitude
                    shootingPartial_1_GeoList.append(temp)

        shootingPartial_2_GeoList = []
        for item in shootingPartial_2:
            for k, v in item.items():
                if k == "location":
                    temp = (float(v["coordinates"][0]), float(v["coordinates"][1])) #Longitude, Latitude
                    shootingPartial_2_GeoList.append(temp)

        initial_1_temp = []
        initial_2_temp = []
        partial_1_initial = []
        partial_2_initial = []
        
        """
        
        #Randomly sample out 8 points from partial 1 as the cluster initial points, and they will be stored into local file and later push to
        #mongodb
        
        
        for i in range(0, 8):
            initial_1_temp.append(shootingPartial_1_GeoList[int(len(shootingPartial_1_GeoList) * numpy.random.rand())])
            initial_2_temp.append(shootingPartial_2_GeoList[int(len(shootingPartial_2_GeoList) * numpy.random.rand())])

        with open('localData\proj2\partial_1_initial.json', 'w') as outfile:
            json.dump(initial_1_temp, outfile)
        with open('localData\proj2\partial_2_initial.json', 'w') as outfile:
            json.dump(initial_2_temp, outfile)
        
        """

        initial_1_temp = []
        initial_2_temp = []
        with open('localData\proj2\partial_1_initial.json') as data_file:
            initial_1_temp = json.load(data_file)
        with open('localData\proj2\partial_2_initial.json') as data_file:
            initial_2_temp = json.load(data_file)

        for l in initial_1_temp:
            partial_1_initial.append((l[0],l[1]))
        for l in initial_2_temp:
            partial_2_initial.append((l[0],l[1]))
        

        shootingCluster1 = coordKMeans(partial_1_initial, shootingPartial_1_GeoList)
        shootingCluster2 = coordKMeans(partial_2_initial, shootingPartial_2_GeoList)


        ## Find the cloest matched crime cluster point for the old cluster point
        C1C2D = [(c1, c2, geoDist(c1,c2)) for (c1, c2) in product(shootingCluster1, shootingCluster2)]
        C1Ds = [(c1, geoDist(c1,c2)) for (c1,c2,d) in C1C2D]
        C1D = aggregate(C1Ds, min)
        #print(CP)
        C1C2 = [(c1, c2) for ((c1,c2,d),(c1_2,d_2)) in product(C1C2D, C1D) if c1 == c1_2 and d == d_2]

        ##Calculate the Difference Vector between two 
        crimeDiffVector = []
        ##print(C1C2)
        for item in C1C2:
            crimeDiffVector.extend([[(item[0]),(100 * (item[1][0] -item[0][0]), 1000 * (item[1][1] -item[0][1]))]])

        ##Match shooting cluster to cloest liquor cluster
        CLD = [(c, l, geoDist(c,l)) for (c, l) in product(shootingCluster1,liquorCluster1)]
        CDs = [(c, geoDist(c,l)) for (c,l,d) in CLD]
        CD = aggregate(CDs, min)
        #print(CP)
        CL = [(c, l) for ((c,l,d),(c_2,d_2)) in product(CLD, CD) if c == c_2 and d == d_2]

        ##Matching the vectors
        ##Following code will calulate the magnitude for the liquor store changing vector.
        vectorMagnitudeChangeInLiquor = []
        for k, v in LiquorDiffVector:
            vectorMagnitudeChangeInLiquor.append(math.sqrt(v[0] * v[0] + v[1] * v[1]))



        ##Following code will perform a projection for each vector of changing shooting crime onto the nearest
        ##vector of changing liquor store vector. Such projection will cancel out the direction effect on the magnitude of the 
        ##vector.
        crimeDiffVectorProjectOnLiquorVector = []
        for item in CL:
            crimeVector = (0,0)
            liquorVector = (0,0)
            for k,v in crimeDiffVector:
                if(item[0] == k):
                    crimeVector = v
            for k,v in LiquorDiffVector:
                if(item[1] == k):
                    liquorVector = v
            crimeDiffVectorProjectOnLiquorVector.append(vectorProject(crimeVector, liquorVector))

        ##Calculate the magnitude of the projected vector.
        vectorMagnitudeChangeInCrimeInDirectionOfLiquor = []
        for v in crimeDiffVectorProjectOnLiquorVector:
            vectorMagnitudeChangeInCrimeInDirectionOfLiquor.append(math.sqrt(v[0] * v[0] + v[1] * v[1]))



        print(vectorMagnitudeChangeInCrimeInDirectionOfLiquor)
        print(vectorMagnitudeChangeInLiquor)


        ##Then calculate the correlation between the vector magnitude in liquor and the vector magnitude in crime
        ##that is in the direction of the cloest liquor vector. (through projection).
        correlation = np.corrcoef(vectorMagnitudeChangeInCrimeInDirectionOfLiquor, vectorMagnitudeChangeInLiquor )
        print("The correlation between two data sets are: ")
        print(correlation[0,1])
        print("And the conclusion we can draw from that is:\n The change of the shooting crime cluster from 2013 to 2016 actually \
            actually does not have a strong correlation to the change of the nearest liquor store cluster")







        ##The Second part of project 2 will be a constraint problem on determining on effective the police station is:
        ##The objective function is pretty obvious, Z = 10 * (distance shooting crimeCluster moved away from police station)
        ##And we are setting the threshhold to be . If the threshold is passed, then we determine the police station as effective.

        rawPolice = repo['ll0406_siboz.policeLocation'].find({});

        ##
        policeGeoList = []
        for item in rawPolice:
            for k, v in item.items():
                if k == "location":
                    temp = (float(v["coordinates"][0]), float(v["coordinates"][1]))    ##Longitude, Latitude
                    policeGeoList.append(temp)

        ## Find the cloest shooting crime point to the police station. (2013)
        PCD = [(p, c, dist(p,c)) for (p, c) in product(policeGeoList, shootingCluster1)]
        PDs = [(p, dist(p,c)) for (p, c, d) in PCD]
        PD = aggregate(PDs, min)

        ## Find the cloest shooting crime point to the police station. (2013-2016)
        PCD_1 = [(p, c, dist(p,c)) for (p, c) in product(policeGeoList, shootingCluster2)]
        PDs_1 = [(p, dist(p,c)) for (p, c, d) in PCD_1]
        PD_1 = aggregate(PDs_1, min)

        PD_change = [(k1, v1-v0) for k0,v0 in PD for k1,v1 in PD_1 if k0 == k1]

        effectiveP = [k for k, v in PD_change if (10 * v > 5)]

        print("According to the constraints given, the effective police stations are at locations: ")
        for k in effectiveP:
            print(k)




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

            this_script = doc.agent('alg:ll0406_siboz#proj2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
            generalTemplate = doc.agent('alg:ll0406_siboz#generalHelperTemplate', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
            dataRetrieval = doc.agent('alg:ll0406_siboz#dataRetrieval', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
            Crim_Resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:Crim_Resource', 'ont:Extension':'json'})
            PoliceLoc_Resource = doc.entity('bdp:pyxn-r3i2', {'prov:label':'Boston Police District Stations', prov.model.PROV_TYPE:'ont:PoliceLoc_Resource', 'ont:Extension':'json'})
            Entertainment_Resource = doc.entity('bdp:cz6t-w69j', {'prov:label':'Entertainment License', prov.model.PROV_TYPE:'ont:Entertainment_Resource', 'ont:Extension':'json'})
            Shooting_crime_Resource = doc.entity('bdp:29yf-ye7n,ufcx-3fdn', {'prov:label':'Shooting Crime', prov.model.PROV_TYPE:'ont:Shooting_crime_Resource', 'ont:Extension':'json'})
            Food_Resource = doc.entity('bdp:fdxy-gydq', {'prov:label':'Restaurant License', prov.model.PROV_TYPE:'ont:Food_Resource', 'ont:Extension':'json'})
            Liquor_Resource = doc.entity('bdp:g9d9-7sj6', {'prov:label':'Liquor License', prov.model.PROV_TYPE:'ont:Liquor_Resource', 'ont:Extension':'json'})

            get_Criminal = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            get_PoliceLoc = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            get_Entertainment = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            get_Food = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            get_ShootingCrime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            get_Liquor = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            compute_CorreCoef = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            compute_EffectivePoliceStation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(get_Criminal, dataRetrieval)
            doc.wasAssociatedWith(get_PoliceLoc, dataRetrieval)
            doc.wasAssociatedWith(get_Entertainment, dataRetrieval)
            doc.wasAssociatedWith(get_Food, dataRetrieval)
            doc.wasAssociatedWith(get_ShootingCrime, dataRetrieval)
            doc.wasAssociatedWith(get_Liquor, dataRetrieval)
            doc.wasAssociatedWith(compute_CorreCoef, this_script)
            doc.wasAssociatedWith(compute_EffectivePoliceStation, this_script)

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
            doc.usage(get_Entertainment, Entertainment_Resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Retrieval',
                    }
                )
            doc.usage(get_Food, Food_Resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Retrieval',
                    }
                )
            doc.usage(get_ShootingCrime, Shooting_crime_Resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Retrieval',
                     'ont:Query':'?shooting=Yes'
                    }
                )
            doc.usage(get_Liquor, Liquor_Resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Retrieval',
                    }
                )
            doc.usage(compute_EffectivePoliceStation, Shooting_crime_Resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Retrieval',
                    }
                )
            doc.usage(compute_CorreCoef, Liquor_Resource, Shooting_crime_Resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute',
                    }
                )

            Criminal = doc.entity('dat:ll0406_siboz#Criminal', {prov.model.PROV_LABEL:'Criminal Location', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(Criminal, dataRetrieval)
            doc.wasGeneratedBy(Criminal, get_Criminal, endTime)
            doc.wasDerivedFrom(Criminal, Crim_Resource, get_Criminal, get_Criminal, get_Criminal)

            PoliceLoc = doc.entity('dat:ll0406_siboz#PoliceLoc', {prov.model.PROV_LABEL:'PoliceStation Location', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(PoliceLoc, dataRetrieval)
            doc.wasGeneratedBy(PoliceLoc, get_PoliceLoc, endTime)
            doc.wasDerivedFrom(PoliceLoc, PoliceLoc_Resource, get_PoliceLoc, get_PoliceLoc, get_PoliceLoc)

            Entertainment = doc.entity('dat:ll0406_siboz#Entertainment', {prov.model.PROV_LABEL:'Entertainment Location', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(Entertainment, dataRetrieval)
            doc.wasGeneratedBy(Entertainment, get_Entertainment, endTime)
            doc.wasDerivedFrom(Entertainment, Entertainment_Resource, get_Entertainment, get_Entertainment, get_Entertainment)

            Liquor = doc.entity('dat:ll0406_siboz#Liquor', {prov.model.PROV_LABEL:'Liquor License', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(Liquor, dataRetrieval)
            doc.wasGeneratedBy(Liquor, get_Liquor, endTime)
            doc.wasDerivedFrom(Liquor, Liquor_Resource, get_Liquor, get_Liquor, get_Liquor)

            ShootingCrime = doc.entity('dat:ll0406_siboz#ShootingCrime', {prov.model.PROV_LABEL:'Entertainment Location', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(Entertainment, dataRetrieval)
            doc.wasGeneratedBy(ShootingCrime, get_ShootingCrime, endTime)
            doc.wasDerivedFrom(ShootingCrime, Shooting_crime_Resource, get_ShootingCrime, get_ShootingCrime, get_ShootingCrime)

            repo.record(doc.serialize()) # Record the provenance document.
            repo.logout()
            return doc

proj2.execute()
doc = proj2.provenance()
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof