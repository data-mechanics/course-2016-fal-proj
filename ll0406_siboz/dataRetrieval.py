import urllib.request
import json
import dml
import numpy
import math
from sodapy import Socrata

def retrieval():
        contributor = 'll0406_siboz'
        reads = []
        writes = ["ll0406_siboz.crimeIncident", "ll0406_siboz.policeLocation", "ll0406_siboz.crimeDistToPoliceSation", "ll0406_siboz.liquorLocation", "ll0406_siboz.foodLocation", "ll0406_siboz.entertainmentLocation"]
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
        ##print(json.dumps(rawCrime, sort_keys=True, indent=4, separators=(',',': ')))

        #Police station location retrieval
        url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json'
        res = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(res)
        repo.dropPermanent("policeLocation")
        repo.createPermanent("policeLocation")
        repo['ll0406_siboz.policeLocation'].insert_many(r)
        ##rawPoliceStation = repo['ll0406_siboz.policeLocation'].find({});


        #Liquor store location retrieval
        url = 'https://data.cityofboston.gov/resource/g9d9-7sj6.json'
        res = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(res)
        url2 = 'https://data.cityofboston.gov/resource/g9d9-7sj6.json?$limit=108&$offset=1001'
        res2 = urllib.request.urlopen(url2).read().decode("utf-8")
        r2 = json.loads(res2)
        repo.dropPermanent("liquorLocation")
        repo.createPermanent("liquorLocation")
        repo['ll0406_siboz.liquorLocation'].insert_many(r)
        repo['ll0406_siboz.liquorLocation'].insert_many(r2)
        ##rawLiquor = repo['ll0406_siboz.liquorLocation'].find({});


        #Restaraunt location retrieval
        url = 'https://data.cityofboston.gov/resource/fdxy-gydq.json'
        res = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(res)
        repo.dropPermanent("foodLocation")
        repo.createPermanent("foodLocation")
        repo['ll0406_siboz.foodLocation'].insert_many(r)
        ##rawFood = repo['ll0406_siboz.foodLocation'].find({});


        #Entertainment location retrieval
        url = 'https://data.cityofboston.gov/resource/cz6t-w69j.json'
        res = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(res)
        repo.dropPermanent("entertainmentLocation")
        repo.createPermanent("entertainmentLocation")
        repo['ll0406_siboz.entertainmentLocation'].insert_many(r)
        ##rawEntertainment = repo['ll0406_siboz.printentertainmentLocation'].find({});

        #New Crime Data Retrieve   Server and Shooting Related, 2012 - up to date
        url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json?shooting=Yes'
        url2 = 'https://data.cityofboston.gov/resource/29yf-ye7n.json?shooting=Y'
        res = urllib.request.urlopen(url).read().decode("utf-8")
        res2 = urllib.request.urlopen(url2).read().decode("utf-8")
        r = json.loads(res)
        r2 = json.loads(res2)
        print(len(r))
        print(len(r2))
        repo.dropPermanent("shootingCrimeIncident")
        repo.createPermanent("shootingCrimeIncident")
        repo['ll0406_siboz.shootingCrimeIncident'].insert_many(r)
        repo['ll0406_siboz.shootingCrimeIncident'].insert_many(r2)


