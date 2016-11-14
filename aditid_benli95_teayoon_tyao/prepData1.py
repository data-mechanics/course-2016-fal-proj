import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from geopy.distance import great_circle   

class prepData1(dml.Algorithm):
    contributor = 'aditid_benli95_teayoon_tyao'
    reads = ['aditid_benli95_teayoon_tyao.allCrimesMaster', 'aditid_benli95_teayoon_tyao.allDrugCrimesMaster' , 'aditid_benli95_teayoon_tyao.childFeedingProgramsTrimmed', 'aditid_benli95_teayoon_tyao.dayCampsdayCaresmaster', 'aditid_benli95_teayoon_tyao.schoolsMaster']
    writes = ['aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius', 'aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug']

    @staticmethod
    def execute(r):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        print("hello from prepData1")

        repo.dropPermanent('aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius')
        repo.createPermanent('aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius')

        repo.dropPermanent('aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug')
        repo.createPermanent('aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug')

        radius = r
        #radius = 5 #miles

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

                thisCrime = {"location": crimeLatLong, "schoolsInRadius": countSchools, "privateSchoolsInRadius": countPrivateSchools, "publicSchoolsInRadius": countPublicSchool, "dayCaresInRadius": countDayCares, "privateDayCaresInRadius": countPrivateDayCares, "publicDayCaresInRaidus": countPublicDayCares, "dayCampsInRadius": countDayCamps , "childFeedingProgramsInRadius": countChildFeedingPrograms, "total": countSchools + countDayCamps + countDayCares + countChildFeedingPrograms}

                res = repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius.insert_one(thisCrime)
    

        crimes = repo.aditid_benli95_teayoon_tyao.allDrugCrimesMaster.find()
        for crime in crimes:
            crimeDict = dict(crime)
            if crimeDict["latitude"] == None or crimeDict["longitude"] == None:
                pass
            else:
                crimeLatLong = (crimeDict["longitude"], crimeDict["latitude"])

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

                thisCrime = {"location": crimeLatLong, "schoolsInRadius": countSchools, "privateSchoolsInRadius": countPrivateSchools, "publicSchoolsInRadius": countPublicSchool, "dayCaresInRadius": countDayCares, "privateDayCaresInRadius": countPrivateDayCares, "publicDayCaresInRaidus": countPublicDayCares, "dayCampsInRadius": countDayCamps , "childFeedingProgramsInRadius": countChildFeedingPrograms, "total": countSchools + countDayCamps + countDayCares + countChildFeedingPrograms}
                    
                res = repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug.insert_one(thisCrime)
                



        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:aditid_benli95_teayoon_tyao#prepData1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        prepD1 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Prep Data 1', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(prepD1, this_script)

        allCrimesMaster = doc.entity('dat:aditid_benli95_teayoon_tyao#allCrimesMaster', {'prov:label':'All Crime Incident Reports', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(prepD1, allCrimesMaster, startTime)

        allDrugCrimesMaster = doc.entity('dat:aditid_benli95_teayoon_tyao#allDrugCrimesMaster', {'prov:label':'All Drug Crime Incident Reports', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(prepD1, allDrugCrimesMaster, startTime)

        childFeedingProgramsTrimmed = doc.entity('dat:aditid_benli95_teayoon_tyao#childFeedingProgramsTrimmed', {'prov:label':'Child Feeding Programs', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(prepD1, childFeedingProgramsTrimmed, startTime)

        dayCampdayCaresMaster = doc.entity('dat:aditid_benli95_teayoon_tyao#dayCampdayCaresMaster', {'prov:label':'Day Camps and Daycares', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(prepD1, dayCampdayCaresMaster, startTime)

        schoolsMaster = doc.entity('dat:aditid_benli95_teayoon_tyao#schoolsMaster', {'prov:label':'All Schools', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(prepD1, schoolsMaster, startTime)

        numberOfEstablishmentsinRadius = doc.entity('dat:aditid_benli95_teayoon_tyao#numberOfEstablishmentsinRadius', {'prov:label':'Number Of Establishments near All Crimes', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.wasAttributedTo(numberOfEstablishmentsinRadius, this_script)
        doc.wasGeneratedBy(numberOfEstablishmentsinRadius, prepD1, endTime)
        doc.wasDerivedFrom(numberOfEstablishmentsinRadius, allCrimesMaster, prepD1, prepD1, prepD1)
        doc.wasDerivedFrom(numberOfEstablishmentsinRadius, childFeedingProgramsTrimmed, prepD1, prepD1, prepD1)
        doc.wasDerivedFrom(numberOfEstablishmentsinRadius, dayCampdayCaresMaster, prepD1, prepD1, prepD1)
        doc.wasDerivedFrom(numberOfEstablishmentsinRadius, schoolsMaster, prepD1, prepD1, prepD1)
        
        numberOfEstablishmentsinRadiusDrug = doc.entity('dat:aditid_benli95_teayoon_tyao#numberOfEstablishmentsinRadiusDrug', {'prov:label':'Number Of Establishments near Drug Crimes', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.wasAttributedTo(numberOfEstablishmentsinRadiusDrug, this_script)
        doc.wasGeneratedBy(numberOfEstablishmentsinRadiusDrug, prepD1, endTime)
        doc.wasDerivedFrom(numberOfEstablishmentsinRadiusDrug, allDrugCrimesMaster, prepD1, prepD1, prepD1)
        doc.wasDerivedFrom(numberOfEstablishmentsinRadiusDrug, childFeedingProgramsTrimmed, prepD1, prepD1, prepD1)
        doc.wasDerivedFrom(numberOfEstablishmentsinRadiusDrug, dayCampdayCaresMaster, prepD1, prepD1, prepD1)
        doc.wasDerivedFrom(numberOfEstablishmentsinRadiusDrug, schoolsMaster, prepD1, prepD1, prepD1)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

#prepData1.execute(3)
#doc = prepData1.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))