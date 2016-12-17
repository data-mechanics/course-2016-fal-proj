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

class part3_2(dml.Algorithm):
    contributor = 'll0406_siboz'
    reads = ['ll0406_siboz.crimeCluster2014', 'll0406_siboz.crimeCluster2015', 'll0406_siboz.crimeCluster2016', 'll0406_siboz.lowValuePropCluster2014', 'll0406_siboz.lowValuePropCluster2015', 'll0406_siboz.lowValuePropCluster2016']
    writes = ['crimeClusterPair_with_lowValuePropClusterPair_14TO15', 'crimeClusterPair_with_lowValuePropClusterPair_15TO16', 'll0406_siboz.corrCoefs_crime_lowValProp']
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
        crimeCluster_2014_cursor = repo['ll0406_siboz.crimeCluster2014'].find({});
        crimeCluster_2015_cursor = repo['ll0406_siboz.crimeCluster2015'].find({});
        crimeCluster_2016_cursor = repo['ll0406_siboz.crimeCluster2016'].find({});
        lowValueCluster_2014_cursor = repo['ll0406_siboz.lowValuePropCluster2014'].find({});
        lowValueCluster_2015_cursor = repo['ll0406_siboz.lowValuePropCluster2015'].find({});
        lowValueCluster_2016_cursor = repo['ll0406_siboz.lowValuePropCluster2016'].find({});

        """
        Following code is for d3 visualization
        
        crimeCluster_2014t = []
        crimeCluster_2015t = []
        crimeCluster_2016t = []
        lowValueCluster_2014t = []
        lowValueCluster_2015t = []
        lowValueCluster_2016t = []
        for item in crimeCluster_2014_cursor:
            temp = [item["coordinates"][0], item["coordinates"][1]]
            crimeCluster_2014t.append(temp)
        for item in crimeCluster_2015_cursor:
            crimeCluster_2015t.append([item["coordinates"][0], item["coordinates"][1]])
        for item in crimeCluster_2016_cursor:
            crimeCluster_2016t.append([item["coordinates"][0], item["coordinates"][1]])
        for item in lowValueCluster_2014_cursor:
            lowValueCluster_2014t.append([item["coordinates"][0], item["coordinates"][1]])
        for item in lowValueCluster_2015_cursor:
            lowValueCluster_2015t.append([item["coordinates"][0], item["coordinates"][1]])
        for item in lowValueCluster_2016_cursor:
            lowValueCluster_2016t.append([item["coordinates"][0], item["coordinates"][1]])
 

        print(crimeCluster_2016t)
        """

        ##Start of part3_2




        crimeCluster_2014 = []
        crimeCluster_2015 = []
        crimeCluster_2016 = []
        lowValueCluster_2014 = []
        lowValueCluster_2015 = []
        lowValueCluster_2016 = []
        for item in crimeCluster_2014_cursor:
            temp = (item["coordinates"][0], item["coordinates"][1])
            crimeCluster_2014.append(temp)
        for item in crimeCluster_2015_cursor:
            crimeCluster_2015.append((item["coordinates"][0], item["coordinates"][1]))
        for item in crimeCluster_2016_cursor:
            crimeCluster_2016.append((item["coordinates"][0], item["coordinates"][1]))
        for item in lowValueCluster_2014_cursor:
            lowValueCluster_2014.append((item["coordinates"][0], item["coordinates"][1]))
        for item in lowValueCluster_2015_cursor:
            lowValueCluster_2015.append((item["coordinates"][0], item["coordinates"][1]))
        for item in lowValueCluster_2016_cursor:
            lowValueCluster_2016.append((item["coordinates"][0], item["coordinates"][1]))



        #print(len(crimeCluster_2014))
        """
        This part examine the cluster change from 2014 to 2015
        """
        ##Matching each 2014 crime cluster with one 2015 crime cluster that has least distance to it.
        C4C5D = [(c4,c5,geoDist(c4,c5)) for (c4, c5) in product(crimeCluster_2014, crimeCluster_2015)]
        C4Ds = [(c4, geoDist(c4, c5)) for (c4, c5, d) in C4C5D]
        C4D = aggregate(C4Ds, min)
        C4C5 = [(c4, c5) for ((c4,c5,d),(c4_1,d_1)) in product(C4C5D, C4D) if c4==c4_1 and d == d_1]

        #Calculate the vector change
        DiffVec_C4C5 = [(100*(c5[0]-c4[0]), 100*(c5[1]-c4[1])) for c4,c5 in C4C5]

        ##Also matching each 2014 crime cluster with one 2014 low value properties cluster
        C4P4D = [(c4,p4,geoDist(c4,p4)) for (c4, p4) in product(crimeCluster_2014, lowValueCluster_2014)]
        C4Ds = [(c4, geoDist(c4, p4)) for (c4, p4, d) in C4P4D]
        C4D = aggregate(C4Ds, min)
        C4P4 = [(c4, p4) for ((c4,p4,d),(c4_1,d_1)) in product(C4P4D, C4D) if c4==c4_1 and d == d_1]

        ##For those low value properties clusters in 2014 got matched, match them to one of the low property cluster in 2015.
        P4 = [p4 for c4, p4 in C4P4]
        #print(P4)
        P4P5D = [(p4,p5,geoDist(p4,p5)) for (p4, p5) in product(P4, lowValueCluster_2015)]
        P4Ds = [(p4, geoDist(p4, p5)) for (p4, p5, d) in P4P5D]
        P4D = aggregate(P4Ds, min)
        P4P5 = [(p4, p5) for ((p4,p5,d),(p4_1,d_1)) in product(P4P5D, P4D) if p4==p4_1 and d == d_1]
        #print(P4P5)
        ##Valculate the vector change
        DiffVec_P4P5 = [(100*(p5[0]-p4[0]), 100*(p5[1]-p4[1])) for p4,p5 in P4P5]

        ##Project the crime change vector on corresponding low value property 
        ProjCrimeChange_1415 = []
        ProjCrimeChangeMag_1415 = [] ##Store the magnitude
        for i in range(0, len(DiffVec_C4C5)):
            v = vectorProject(DiffVec_C4C5[i], DiffVec_P4P5[i])
            ProjCrimeChange_1415.append(v)
            ProjCrimeChangeMag_1415.append(math.sqrt(v[0] * v[0] + v[1] * v[1]))

        PropertyDiffMag_1415 = []
        for v in DiffVec_P4P5:
            PropertyDiffMag_1415.append(math.sqrt(v[0] * v[0] + v[1] * v[1]))

        print(PropertyDiffMag_1415)
        print(ProjCrimeChangeMag_1415)
        correlationCoef_1415 = np.corrcoef(ProjCrimeChangeMag_1415, PropertyDiffMag_1415)
       # print(correlation[0,1])

        """
        plt.scatter(PropertyDiffMag_1415, ProjCrimeChangeMag_1415)
        plt.title('|Proj_u| vs |v| (14-15)')
        plt.show()
        print(correlationCoef_1415[0, 1])
        """


        """
        Now we will examine the time period of 2015-2016
        """
        ##Matching each 2015 crime cluster with one 2016 crime cluster that has least distance to it.
        C5C6D = [(c5,c6,geoDist(c5,c6)) for (c5, c6) in product(crimeCluster_2015, crimeCluster_2016)]
        C5Ds = [(c5, geoDist(c5, c6)) for (c5, c6, d) in C5C6D]
        C5D = aggregate(C5Ds, min)
        C5C6 = [(c5, c6) for ((c5,c6,d),(c5_1,d_1)) in product(C5C6D, C5D) if c5==c5_1 and d == d_1]

        #Calculate the vector change
        DiffVec_C5C6 = [(100*(c6[0]-c5[0]), 100*(c6[1]-c5[1])) for c5,c6 in C5C6]

        ##Also matching each 2015 crime cluster with one 2015 low value properties cluster
        C5P5D = [(c5,p5,geoDist(c5,p5)) for (c5, p5) in product(crimeCluster_2015, lowValueCluster_2015)]
        C5Ds = [(c5, geoDist(c5, p5)) for (c5, p5, d) in C5P5D]
        C5D = aggregate(C5Ds, min)
        C5P5 = [(c5, p5) for ((c5,p5,d),(c5_1,d_1)) in product(C5P5D, C5D) if c5==c5_1 and d == d_1]

        ##For those low value properties clusters in 2015 got matched, match them to one of the low property cluster in 2016.
        P5 = [p5 for c5, p5 in C5P5]
        P5P6D = [(p5,p6,geoDist(p5,p6)) for (p5, p6) in product(P5, lowValueCluster_2016)]
        P5Ds = [(p5, geoDist(p5, p6)) for (p5, p6, d) in P5P6D]
        P5D = aggregate(P5Ds, min)
        P5P6 = [(p5, p6) for ((p5,p6,d),(p5_1,d_1)) in product(P5P6D, P5D) if p5==p5_1 and d == d_1]

        #print(P5P6)
        ##Valculate the vector change
        DiffVec_P5P6 = [(100*(p6[0]-p5[0]), 100*(p6[1]-p5[1])) for p5,p6 in P5P6]

        ##Project the crime change vector on corresponding low value property 
        ProjCrimeChange_1516 = []
        ProjCrimeChangeMag_1516 = [] ##Store the magnitude
        for i in range(0, len(DiffVec_C5C6)):
            v = vectorProject(DiffVec_C5C6[i], DiffVec_P5P6[i])
            ProjCrimeChange_1516.append(v)
            ProjCrimeChangeMag_1516.append(math.sqrt(v[0] * v[0] + v[1] * v[1]))

        PropertyDiffMag_1516 = []
        for v in DiffVec_P5P6:
            PropertyDiffMag_1516.append(math.sqrt(v[0] * v[0] + v[1] * v[1]))

        correlationCoef_1516 = np.corrcoef(ProjCrimeChangeMag_1516, PropertyDiffMag_1516)
        


        plt.scatter(PropertyDiffMag_1516, ProjCrimeChangeMag_1516)
        plt.title('|Proj_u| vs |v| (15-16)')
        #plt.plot(range(2))
        plt.show()
        print(correlationCoef_1516[0, 1])
        

        print("start time is: " + str(startTime))
        print("end time is: " + str(datetime.datetime.now()))


        ##Store results into a json file and put into mongo repo
        ##C4C5 with P4P5
        repo.dropPermanent("crimeClusterPair_with_lowValuePropClusterPair_14TO15")
        repo.createPermanent("crimeClusterPair_with_lowValuePropClusterPair_14TO15")
        for i in range(0, 8):
            temp = {"crimePair": [C4C5[i][0], C4C5[i][1]], "lowValuePropPair":[P4P5[i][0], P4P5[i][1]]}
            repo['ll0406_siboz.crimeClusterPair_with_lowValuePropClusterPair_14TO15'].insert_one(temp)

        repo.dropPermanent("crimeClusterPair_with_lowValuePropClusterPair_15TO16")
        repo.createPermanent("crimeClusterPair_with_lowValuePropClusterPair_15TO16")
        for i in range(0, 8):
            temp = {"crimePair": [C5C6[i][0], C5C6[i][1]], "lowValuePropPair":[P5P6[i][0], P5P6[i][1]]}
            repo['ll0406_siboz.crimeClusterPair_with_lowValuePropClusterPair_15TO16'].insert_one(temp)

        repo.dropPermanent("corrCoefs_crime_lowValProp")
        repo.createPermanent("corrCoefs_crime_lowValProp")
        repo['ll0406_siboz.corrCoefs_crime_lowValProp'].insert_one({"2014_2015":correlationCoef_1415[0,1]})
        repo['ll0406_siboz.corrCoefs_crime_lowValProp'].insert_one({"2015_2016":correlationCoef_1516[0,1]})


        pointer1 = repo['ll0406_siboz.crimeClusterPair_with_lowValuePropClusterPair_14TO15'].find({});
        pointer2 = repo['ll0406_siboz.crimeClusterPair_with_lowValuePropClusterPair_15TO16'].find({});


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
            
            crimeCluster2014 = doc.entity('dat:ll0406_siboz#crimeCluster2014', {prov.model.PROV_LABEL:'Crime Cluster 2014', prov.model.PROV_TYPE:'ont:DataSet'})
            crimeCluster2015 = doc.entity('dat:ll0406_siboz#crimeCluster2015', {prov.model.PROV_LABEL:'Crime Cluster 2015', prov.model.PROV_TYPE:'ont:DataSet'})
            crimeCluster2016 = doc.entity('dat:ll0406_siboz#crimeCluster2016', {prov.model.PROV_LABEL:'Crime Cluster 2016', prov.model.PROV_TYPE:'ont:DataSet'})
            lowValuePropCluster2014 = doc.entity('dat:ll0406_siboz#lowValuePropCluster2014', {prov.model.PROV_LABEL:'Low Value Properties Cluster 2014', prov.model.PROV_TYPE:'ont:DataSet'})
            lowValuePropCluster2015 = doc.entity('dat:ll0406_siboz#lowValuePropCluster2015', {prov.model.PROV_LABEL:'Low Value Properties Cluster 2015', prov.model.PROV_TYPE:'ont:DataSet'})
            lowValuePropCluster2016 = doc.entity('dat:ll0406_siboz#lowValuePropCluster2016', {prov.model.PROV_LABEL:'Low Value Properties Cluster 2016', prov.model.PROV_TYPE:'ont:DataSet'})

            compute1415Pair = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            compute1516Pair = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            computeCorrCoeff = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(compute1415Pair, this_script)
            doc.wasAssociatedWith(compute1516Pair, this_script)

            doc.usage(compute1415Pair, crimeCluster2014, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(compute1415Pair, crimeCluster2015, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(compute1415Pair, lowValuePropCluster2014, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(compute1415Pair, lowValuePropCluster2015, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(compute1516Pair, crimeCluster2015, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(compute1516Pair, crimeCluster2016, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(compute1516Pair, lowValuePropCluster2015, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )
            doc.usage(compute1516Pair, lowValuePropCluster2016, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Compute'}
                )


            crimeClusterPair_with_lowValuePropClusterPair_14TO15 = doc.entity('dat:ll0406_siboz#crimeClusterPair_with_lowValuePropClusterPair_14TO15', {prov.model.PROV_LABEL:'Crime Pair and Low Val Property Pair 14-15', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(crimeClusterPair_with_lowValuePropClusterPair_14TO15, this_script)
            doc.wasGeneratedBy(crimeClusterPair_with_lowValuePropClusterPair_14TO15, compute1415Pair, endTime)
            doc.wasDerivedFrom(crimeClusterPair_with_lowValuePropClusterPair_14TO15, crimeCluster2014, lowValuePropCluster2014, compute1415Pair, compute1415Pair, compute1415Pair)

            crimeClusterPair_with_lowValuePropClusterPair_15TO16 = doc.entity('dat:ll0406_siboz#crimeClusterPair_with_lowValuePropClusterPair_15TO16', {prov.model.PROV_LABEL:'Crime Pair and Low Val Property Pair 15-16', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(crimeClusterPair_with_lowValuePropClusterPair_15TO16, this_script)
            doc.wasGeneratedBy(crimeClusterPair_with_lowValuePropClusterPair_15TO16, compute1516Pair, endTime)
            doc.wasDerivedFrom(crimeClusterPair_with_lowValuePropClusterPair_15TO16, crimeCluster2016, lowValuePropCluster2016, compute1516Pair, compute1516Pair, compute1516Pair)

            corrCoefs_crime_lowValProp = doc.entity('dat:ll0406_siboz#corrCoefs_crime_lowValProp', {prov.model.PROV_LABEL:'Low value cluster effect crime cluster - corrCoeff', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(corrCoefs_crime_lowValProp, this_script)
            doc.wasGeneratedBy(corrCoefs_crime_lowValProp, compute1516Pair, endTime)
            doc.wasDerivedFrom(corrCoefs_crime_lowValProp, crimeClusterPair_with_lowValuePropClusterPair_14TO15, crimeClusterPair_with_lowValuePropClusterPair_15TO16, computeCorrCoeff, computeCorrCoeff, computeCorrCoeff)


            repo.record(doc.serialize()) # Record the provenance document.
            repo.logout()

            return doc

part3_2.execute()
doc = part3_2.provenance()
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof