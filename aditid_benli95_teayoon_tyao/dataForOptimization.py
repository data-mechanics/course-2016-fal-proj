
# coding: utf-8

# In[21]:

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
    def execute(r, trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        print("hello from prepData1")

        
        """
        The following code drops and creates 10 repositories. So if the radius given is 5 the repositories
        created are 50, 51, ..., 59 for the radius's 5.0, 5.1, ... 5.9 miles respectively.
        """
        
        """
        CHANGE THIS VALUE OF RADIUS HERE!!
        WE'RE DOING IT ALPHABETICALLY:
        ADITI - 1
        ANDREW - 2
        BEN - 3
        TONY - 4
        
        OTHER VALUES IF YOU HAVE TIME:
        0, 5, 6, 7(MAYBE)
        """
        
        
        #radius = r
        radius = 5 #miles
        radius_arr = []
        
        base_string_all = 'aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius'
        base_string_drugs = 'aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug'
        
        for k in range(0,10):
            radius_denom = radius + (k/10)
            radius_arr.append(radius_denom)    #saves float values in array (5.0, 5.1, ...)
            
            add_on = str(radius_denom*10)      #creates string of integer values (50, 51, ...)
            repo_string_all = base_string_all + add_on
            repo_string_drug = base_string_drugs + add_on
            
            repo.dropPermanent(repo_string_all)
            repo.createPermanent(repo_string_all)

            repo.dropPermanent(repo_string_drug)
            repo.createPermanent(repo_string_drug)


        if (trial == True):
            crime = repo.aditid_benli95_teayoon_tyao.allCrimesMaster.aggregate([ { sample: { size: 50 } } ])
        else:
            crimes = repo.aditid_benli95_teayoon_tyao.allCrimesMaster.find()
        for crime in crimes:
            crimeDict = dict(crime)

            if crimeDict["latitude"] == None or crimeDict["longitude"] == None:
                pass
            else:
                crimeLatLong = (crimeDict["latitude"], crimeDict["longitude"])

                countSchools = [0] * 10
                countPrivateSchools = [0] * 10
                countPublicSchool = [0] * 10
                countDayCares = [0] * 10
                countDayCamps = [0] * 10
                countPrivateDayCares = [0] * 10
                countPublicDayCares = [0] * 10
                countChildFeedingPrograms = [0] * 10

                schools = repo.aditid_benli95_teayoon_tyao.schoolsMaster.find()
                for school in schools:
                    schoolDict = dict(school)
                    schoolLatLong = (schoolDict["latitude"], schoolDict["longitude"])

                    dist = great_circle(crimeLatLong, schoolLatLong).miles

                    if dist <= (radius + 0.9):
                        for num in range(0,10):
                            if dist <= radius_arr[num]:
                                countSchools[num] += 1
                                if schoolDict["type"] == "public":
                                    countPublicSchool[num] += 1
                                else:
                                    countPrivateSchools[num] += 1

                dayCampdayCares = repo.aditid_benli95_teayoon_tyao.dayCampdayCaresMaster.find()
                for dayCampdayCare in dayCampdayCares:
                    dayCampdayCareLatLong = (dayCampdayCare["latitude"], dayCampdayCare["longitude"])

                    dist = great_circle(crimeLatLong, dayCampdayCareLatLong).miles

                    if dist <= (radius + 0.9):
                        for num in range(0,10):
                            if dist <= radius_arr[num]:
                                if dayCampdayCare["type"] == "private daycare":
                                    countDayCares[num] += 1
                                    countPrivateDayCares[num] += 1
                                if dayCampdayCare["type"] == "public daycare":
                                    countDayCares[num] += 1
                                    countPublicDayCares[num] += 1
                                if dayCampdayCare["type"] == "day camp":
                                    countDayCamps[num] += 1

                childFeedingPrograms = repo.aditid_benli95_teayoon_tyao.childFeedingProgramsTrimmed.find()
                for program in childFeedingPrograms:
                    programLatLong = (program['latitude'], program['longitude'])

                    dist = great_circle(crimeLatLong, programLatLong).miles

                    if dist <= (radius + 0.9):
                        for num in range(0,10):
                            if dist <= radius_arr[num]:
                                countChildFeedingPrograms[num] += 1
                                
                base_string_1 = "repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius"
                base_string_2 = ".insert_one(thisCrime)"
                for num in range(0,10):
                    thisCrime = {"location": crimeLatLong, "schoolsInRadius": countSchools[num], "privateSchoolsInRadius": countPrivateSchools[num], "publicSchoolsInRadius": countPublicSchool[num], "dayCaresInRadius": countDayCares[num], "privateDayCaresInRadius": countPrivateDayCares[num], "publicDayCaresInRaidus": countPublicDayCares[num], "dayCampsInRadius": countDayCamps[num], "childFeedingProgramsInRadius": countChildFeedingPrograms[num], "total": countSchools[num] + countDayCamps[num] + countDayCares[num] + countChildFeedingPrograms[num]}
                
                    add_on = (radius * 10) + num
                    whole_string = base_string_1 + str(add_on) + base_string_2
                    exec(whole_string)
    


        if (trial == True):
            crime = repo.aditid_benli95_teayoon_tyao.allDrugCrimesMaster.aggregate([ { sample: { size: 50 } } ])
        else:
            crimes = repo.aditid_benli95_teayoon_tyao.allDrugCrimesMaster.find()
        for crime in crimes:
            crimeDict = dict(crime)
            if crimeDict["latitude"] == None or crimeDict["longitude"] == None:
                pass
            else:
                crimeLatLong = (crimeDict["longitude"], crimeDict["latitude"])

                countSchools = [0] * 10
                countPrivateSchools = [0] * 10
                countPublicSchool = [0] * 10
                countDayCares = [0] * 10
                countDayCamps = [0] * 10
                countPrivateDayCares = [0] * 10
                countPublicDayCares = [0] * 10
                countChildFeedingPrograms = [0] * 10

                schools = repo.aditid_benli95_teayoon_tyao.schoolsMaster.find()
                for school in schools:
                    schoolDict = dict(school)
                    schoolLatLong = (schoolDict["latitude"], schoolDict["longitude"])

                    dist = great_circle(crimeLatLong, schoolLatLong).miles

                    if dist <= (radius + 0.9):
                        for num in range(0,10):
                            if dist <= radius_arr[num]:
                                countSchools[num] += 1
                                if schoolDict["type"] == "public":
                                    countPublicSchool[num] += 1
                                else:
                                    countPrivateSchools[num] += 1

                dayCampdayCares = repo.aditid_benli95_teayoon_tyao.dayCampdayCaresMaster.find()
                for dayCampdayCare in dayCampdayCares:
                    dayCampdayCareLatLong = (dayCampdayCare["latitude"], dayCampdayCare["longitude"])

                    dist = great_circle(crimeLatLong, dayCampdayCareLatLong).miles

                    if dist <= (radius + 0.9):
                        for num in range(0,10):
                            if dist <= radius_arr[num]:
                                if dayCampdayCare["type"] == "private daycare":
                                    countDayCares[num] += 1
                                    countPrivateDayCares[num] += 1
                                if dayCampdayCare["type"] == "public daycare":
                                    countDayCares[num] += 1
                                    countPublicDayCares[num] += 1
                                if dayCampdayCare["type"] == "day camp":
                                    countDayCamps[num] += 1

                childFeedingPrograms = repo.aditid_benli95_teayoon_tyao.childFeedingProgramsTrimmed.find()
                for program in childFeedingPrograms:
                    programLatLong = (program['latitude'], program['longitude'])

                    dist = great_circle(crimeLatLong, programLatLong).miles

                    if dist <= (radius + 0.9):
                        for num in range(0,10):
                            if dist <= radius_arr[num]:
                                countChildFeedingPrograms[num] += 1

                base_string_1 = "repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug"
                base_string_2 = ".insert_one(thisCrime)"
                for num in range(0,10):
                    
                    thisCrime = {"location": crimeLatLong, "schoolsInRadius": countSchools[num], "privateSchoolsInRadius": countPrivateSchools[num], "publicSchoolsInRadius": countPublicSchool[num], "dayCaresInRadius": countDayCares[num], "privateDayCaresInRadius": countPrivateDayCares[num], "publicDayCaresInRaidus": countPublicDayCares[num], "dayCampsInRadius": countDayCamps[num], "childFeedingProgramsInRadius": countChildFeedingPrograms[num], "total": countSchools[num] + countDayCamps[num] + countDayCares[num] + countChildFeedingPrograms[num]}
                
                    add_on = (radius * 10) + num
                    whole_string = base_string_1 + str(add_on) + base_string_2
                    exec(whole_string)

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

prepData1.execute(3) #IGNORE THIS VALUE
#doc = prepData1.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

