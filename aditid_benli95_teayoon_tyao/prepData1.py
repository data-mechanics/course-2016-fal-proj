import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from geopy.distance import great_circle   

class prepData1(dml.Algorithm):
    contributor = 'aditid_benli95_teayoon_tyao'
    reads = ['aditid_benli95_teayoon_tyao.allCrimesMaster', 'aditid_benli95_teayoon_tyao.childFeedingProgramsTrimmed', 'aditid_benli95_teayoon_tyao.dayCampsdayCaresmaster', 'aditid_benli95_teayoon_tyao.schoolsMaster']
    writes = ['aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        repo.dropPermanent('aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius')
        repo.createPermanent('aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius')

        radius = 5 #miles

        crimes = repo.aditid_benli95_teayoon_tyao.allCrimesMaster.find()
        for crime in crimes:
            crimeDict = dict(crime)

            if crimeDict["latitude"] == None or crimeDict["longitude"] == None:
                pass
            else:
                crimeLatLong = (crimeDict["latitude"], crimeDict["longitude"])

                countSchools = 0
                countPrivateSchools = 0
                countPublicSchool = 0
                countDayCares = 0
                countDayCamps = 0
                countPrivateDayCares = 0
                countPublicDayCares = 0
                countChildFeedingPrograms = 0

                schools = repo.aditid_benli95_teayoon_tyao.schoolsMaster.find()
                for school in schools:
                    schoolDict = dict(school)
                    schoolLatLong = (schoolDict["latitude"], schoolDict["longitude"])

                    dist = great_circle(crimeLatLong, schoolLatLong).miles

                    if dist <= radius:
                        countSchools += 1
                        if schoolDict["type"] == "public":
                            countPublicSchool += 1
                        else:
                            countPrivateSchools += 1

                dayCampdayCares = repo.aditid_benli95_teayoon_tyao.dayCampdayCaresMaster.find()
                for dayCampdayCare in dayCampdayCares:
                    dayCampdayCareLatLong = (dayCampdayCare["latitude"], dayCampdayCare["longitude"])

                    dist = great_circle(crimeLatLong, dayCampdayCareLatLong).miles

                    if dist <= radius:
                        if dayCampdayCare["type"] == "private daycare":
                            countDayCares += 1
                            countPrivateDayCares += 1
                        if dayCampdayCare["type"] == "public daycare":
                            countDayCares += 1
                            countPublicDayCares += 1
                        if dayCampdayCare["type"] == "day camp":
                            countDayCamps += 1

                childFeedingPrograms = repo.aditid_benli95_teayoon_tyao.childFeedingProgramsTrimmed.find()
                for program in childFeedingPrograms:
                    programLatLong = (program['latitude'], program['longitude'])

                    dist = great_circle(crimeLatLong, programLatLong).miles

                    if dist <= radius:
                        countChildFeedingPrograms += 1

                thisCrime = {"location": crimeLatLong, "schoolsInRadius": countSchools, "privateSchoolsInRadius": countPrivateSchools, "publicSchoolsInRadius": countPublicSchool, "dayCaresInRadius": countDayCares, "privateDayCaresInRadius": countPrivateDayCares, "publicDayCaresInRaidus": countPublicDayCares, "dayCampsInRadius": countDayCamps , "childFeedingProgramsInRadius": countChildFeedingPrograms , "isDrugCrime": crimeDict["isDrugCrime"]}
                    
                print(thisCrime)
                res = repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius.insert_one(thisCrime)
    
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')
        pass

prepData1.execute()
doc = prepData1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))