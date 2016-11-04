import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import numpy
import math
from sodapy import Socrata

##Some standard transformation
def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def printjson(a):
    print(json.dumps(a, sort_keys=True, indent=4, separators=(',',': ')))

def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

##K-means clustering algorithm for computing coordinate system.
def COORDKMEANS (initial, points):
    M = initial
    P = points
    OLD = []
    #DIFF = []
    notEqual = True
    while notEqual:
        ##print(OLD == M)
        ct = 0
        if OLD != []:
            for i in range(0, len(M)):
                print(i)
                if(math.isclose(OLD[i][0], M[i][0], rel_tol=1e-8) and math.isclose(OLD[i][1], M[i][1], rel_tol=1e-8)):
                    ct = ct + 1
                    if(i == len(M) - 1):
                        notEqual = False
                    continue
                else:
                    print(str(i) + " Something wrong")
                    break
        print(ct)
        OLD = M
        #print("Start")
        MPD = [(m, p, dist(m,p)) for (m, p) in product(M, P)]
        PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
        PD = aggregate(PDs, min)
        MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MT = aggregate(MP, plus)
        M1 = [(m, 1) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MC = aggregate(M1, sum)
        M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]
        print(sorted(M))

    return M

UPPER_LATITUDE = 42.40851
UPPER_LONGITUDE = -70.993137
LOWER_LATITUDE = 42.211017
LOWER_LONGITUDE = -71.138191


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

        #Socrata API setup and raw data retrieval
        socrataClient = Socrata(DOMAIN, None)

        #Crime data retrieval
        #Due to the large size of data set, using the URL request here.
        url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json?$where=location.longitude!=0'
        res = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(res)
        repo.dropPermanent("crimeIncident")
        repo.createPermanent("crimeIncident")
        repo['ll0406_siboz.crimeIncident'].insert_many(r)
        rawCrime = repo['ll0406_siboz.crimeIncident'].find({}); #RawCrime is a cursor
        ##print(json.dumps(rawCrime, sort_keys=True, indent=4, separators=(',',': ')))

        #Police station location retrieval
        url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json'
        res = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(res)
        repo.dropPermanent("policeLocation")
        repo.createPermanent("policeLocation")
        repo['ll0406_siboz.policeLocation'].insert_many(r)
        rawPoliceStation = repo['ll0406_siboz.policeLocation'].find({});

        """
        #Liquor store location retrieval
        url = 'https://data.cityofboston.gov/resource/g9d9-7sj6.json'
        res = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(res)
        repo.createPermanent("liquorLocation")
        repo['ll0406_siboz.liquorLocation'].insert_many(r)
        rawLiquor = repo['ll0406_siboz.liquorLocation'].find({});
        
        
        #Restaraunt location retrieval
        url = 'https://data.cityofboston.gov/resource/fdxy-gydq.json'
        res = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(res)
        repo.dropPermanent("foodLocation")
        repo.createPermanent("foodLocation")
        repo['ll0406_siboz.foodLocation'].insert_many(r)
        rawFood = repo['ll0406_siboz.foodLocation'].find({});

        
        #Entertainment location retrieval
        url = 'https://data.cityofboston.gov/resource/cz6t-w69j.json'
        res = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(res)
        repo.dropPermanent("entertainmentLocation")
        repo.createPermanent("entertainmentLocation")
        repo['ll0406_siboz.entertainmentLocation'].insert_many(r)
        rawEntertainment = repo['ll0406_siboz.printentertainmentLocation'].find({});
        """

        ##Data Cleanup, mainly through selection
        ##Crime data
        CrimeModifiedData = []
        for item in rawCrime:
            temp = {k:v for k,v in item.items() if (k == "shooting" or k == "incident_type_description" or k == "location")} 
            CrimeModifiedData.append(temp)


        #Since the data set is still too large to for computing a k-means clutering algorithm.
        #The program will further randomly select 2000 samples from all the data points.

        #Create a random indicies array 
        randomInd = []
        for i in range (0, 1000):
            randomInd.append(int(len(CrimeModifiedData) * numpy.random.rand()))

        SampleCrimeData = []
        for i in range (0, 1000):
            SampleCrimeData.append(CrimeModifiedData[randomInd[i]])

        crimeGeoList = []
        for item in SampleCrimeData:
            for k, v in item.items():
                if k == "location":
                    temp = (float(v["coordinates"][0]), float(v["coordinates"][1])) #Longitude, Latitude
                    crimeGeoList.append(temp)

        ##We want to see does the police stations' location have some impact on the occurance of
        ##crime event. There are 12 Boston Police Stations, so I will be randomly generating 12 starting points
        ##within the region (LOWER_LONGITUDE, LOWER_LATITUDE) to (UPPER_LONGITUDE, UPPER_LATITUDE) as the starting cluster points for k-means cluster algorithm
        initialPoints = []
        for i in range(0, 12):
            latiOffset = numpy.random.rand() * (UPPER_LATITUDE- LOWER_LATITUDE)
            longiOffset = numpy.random.rand() * (UPPER_LONGITUDE - LOWER_LONGITUDE)
            initialPoints.append((LOWER_LONGITUDE + longiOffset, LOWER_LATITUDE + latiOffset))

        crimeCluster = COORDKMEANS(initialPoints, crimeGeoList)


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


        #Credit to https://gist.github.com/rochacbruno/2883505#file-haversine-py-L6
        def geoDist(p1,p2):
            long1, lat1 = p1
            long2, lat2 = p2
            radius = 6371 #in kilometer
            dlon = math.radians(long1-long2)
            dlat = math.radians(lat1-lat2)
            a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
                * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            d = radius * c
            return d

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