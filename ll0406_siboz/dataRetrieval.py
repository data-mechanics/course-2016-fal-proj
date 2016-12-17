import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
from pprint import pprint
from sodapy import Socrata

class dataRetrieval(dml.Algorithm):
        contributor = 'll0406_siboz'
        reads = []
        writes = ["ll0406_siboz.entertainmentLocation", "ll0406_siboz.crimeIncident", "ll0406_siboz.newCrimeIncident", "ll0406_siboz.policeLocation", 
                "ll0406_siboz.liquorLocation", "ll0406_siboz.foodLocation", "ll0406_siboz.shootingCrimeIncident", "ll0406_siboz.propAssess2014",
                "ll0406_siboz.propAssess2015", "ll0406_siboz.propAssess2016"]
        DOMAIN = "data.cityofboston.gov"

        @staticmethod
        def execute(trail = False):
                startTime = datetime.datetime.now()
                #Setup starts here
                DOMAIN = "data.cityofboston.gov"
                client = dml.pymongo.MongoClient()
                repo = client.repo
                repo.authenticate('ll0406_siboz', 'll0406_siboz')
                client = dml.pymongo.MongoClient()
                repo = client.repo
                repo.authenticate('ll0406_siboz', 'll0406_siboz')

                #Socrata API setup and raw data retrieval
                socrataClient = Socrata(DOMAIN, None)

                """
                #Entertainment location retrieval
                repo.dropPermanent("entertainmentLocation")
                repo.createPermanent("entertainmentLocation")
                for i in range(0, 6):
                        url = 'https://data.cityofboston.gov/resource/cz6t-w69j.json?$offset=' + str(i * 1000 + 1)
                        res = urllib.request.urlopen(url).read().decode("utf-8")
                        r = json.loads(res)
                        repo['ll0406_siboz.entertainmentLocation'].insert_many(r)

                #Crime data retrieval (2012 - 2015/8)
                repo.dropPermanent("crimeIncident")
                repo.createPermanent("crimeIncident")
                for i in range(0, 269):
                        url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json?$offset=' + str(i * 1000 + 1)
                        res = urllib.request.urlopen(url).read().decode("utf-8")
                        r = json.loads(res)
                        repo['ll0406_siboz.crimeIncident'].insert_many(r)
                ##print(json.dumps(rawCrime, sort_keys=True, indent=4, separators=(',',': '))
                
                
                #Crime data retrieval (2015/8 - to date)
                repo.dropPermanent("newCrimeIncident")
                repo.createPermanent("newCrimeIncident")
                for i in range(0, 145):
                        url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json?$offset=' + str(i * 1000 + 1)
                        res = urllib.request.urlopen(url).read().decode("utf-8")
                        r = json.loads(res)
                        repo['ll0406_siboz.newCrimeIncident'].insert_many(r)
                
                

                
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


                #New Crime Data Retrieve   severe and Shooting Related, 2012 - up to date
                url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json?shooting=Yes'
                url2 = 'https://data.cityofboston.gov/resource/29yf-ye7n.json?shooting=Y'
                res = urllib.request.urlopen(url).read().decode("utf-8")
                res2 = urllib.request.urlopen(url2).read().decode("utf-8")
                r = json.loads(res)
                r2 = json.loads(res2)
                repo.dropPermanent("shootingCrimeIncident")
                repo.createPermanent("shootingCrimeIncident")
                repo['ll0406_siboz.shootingCrimeIncident'].insert_many(r)
                repo['ll0406_siboz.shootingCrimeIncident'].insert_many(r2)


                
                #Property Assessment Data (2014)
                repo.dropPermanent("propAssess2014")
                repo.createPermanent("propAssess2014")
                for i in range(0, 165):
                        url = 'https://data.cityofboston.gov/resource/jsri-cpsq.json?$offset=' + str(i * 1000 + 1)
                        res = urllib.request.urlopen(url).read().decode("utf-8")
                        r = json.loads(res)
                        repo['ll0406_siboz.propAssess2014'].insert_many(r)

                #Property Assessment Data (2015)
                repo.dropPermanent("propAssess2015")
                repo.createPermanent("propAssess2015")
                for i in range(0, 169):
                        url = 'https://data.cityofboston.gov/resource/n7za-nsjh.json?$offset=' + str(i * 1000 + 1)
                        res = urllib.request.urlopen(url).read().decode("utf-8")
                        r = json.loads(res)
                        repo['ll0406_siboz.propAssess2015'].insert_many(r)

                
                #Property Assessment Data (2016)
                repo.dropPermanent("propAssess2016")
                repo.createPermanent("propAssess2016")
                for i in range(0, 170):
                        url = 'https://data.cityofboston.gov/resource/g5b5-xrwi.json?$offset=' + str(i * 1000 + 1)
                        res = urllib.request.urlopen(url).read().decode("utf-8")
                        r = json.loads(res)
                        repo['ll0406_siboz.propAssess2016'].insert_many(r)
                """

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

                this_script = doc.agent('alg:ll0406_siboz#dataRetrieval', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
                Crime_Resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Crime Incident Reports 2012-2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                Crime_Resource_New = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime Incident Reports 2015-2016', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                PoliceLoc_Resource = doc.entity('bdp:pyxn-r3i2', {'prov:label':'Boston Police District Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                Entertainment_Resource = doc.entity('bdp:cz6t-w69j', {'prov:label':'Entertainment License', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                Shooting_crime_Resource = doc.entity('bdp:29yf-ye7n,ufcx-3fdn', {'prov:label':'Shooting Crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                Food_Resource = doc.entity('bdp:fdxy-gydq', {'prov:label':'Restaurant License', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                Liquor_Resource = doc.entity('bdp:g9d9-7sj6', {'prov:label':'Liquor License', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                PropertyData2014_Resource = doc.entity('bdp:jsri-cpsq', {'prov:label':'Property Assessment 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                PropertyData2015_Resource = doc.entity('bdp:n7za-nsjh', {'prov:label':'Property Assessment 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                PropertyData2016_Resource = doc.entity('bdp:g5b5-xrwi', {'prov:label':'Property Assessment 2016', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

                get_CrimeData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
                get_NewCrimeData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
                get_Entertainment = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
                get_Food = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
                get_Liquor = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
                get_PoliceLoc = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
                get_ShootingCrime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
                get_Property2014 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
                get_Property2015 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
                get_Property2016 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

                doc.wasAssociatedWith(get_CrimeData, this_script)
                doc.wasAssociatedWith(get_NewCrimeData, this_script)
                doc.wasAssociatedWith(get_Entertainment, this_script)
                doc.wasAssociatedWith(get_Food, this_script)
                doc.wasAssociatedWith(get_Liquor, this_script)
                doc.wasAssociatedWith(get_ShootingCrime, this_script)
                doc.wasAssociatedWith(get_Property2014, this_script)
                doc.wasAssociatedWith(get_Property2015, this_script)
                doc.wasAssociatedWith(get_Property2016, this_script)


                doc.usage(get_CrimeData, Crime_Resource, startTime, None,
                        {prov.model.PROV_TYPE:'ont:Retrieval',
                         'ont:Query':''
                        }
                    )

                doc.usage(get_NewCrimeData, Crime_Resource_New, startTime, None,
                        {prov.model.PROV_TYPE:'ont:Retrieval',
                         'ont:Query':''
                        }
                    )
                doc.usage(get_Entertainment, Entertainment_Resource, startTime, None,
                        {prov.model.PROV_TYPE:'ont:Retrieval',
                         'ont:Query':''
                        }
                    )
                doc.usage(get_Food, Food_Resource, startTime, None,
                        {prov.model.PROV_TYPE:'ont:Retrieval',
                         'ont:Query':''
                        }
                    )
                doc.usage(get_Liquor, Liquor_Resource, startTime, None,
                        {prov.model.PROV_TYPE:'ont:Retrieval',
                         'ont:Query':''
                        }
                    )
                doc.usage(get_ShootingCrime, Shooting_crime_Resource, startTime, None,
                        {prov.model.PROV_TYPE:'ont:Retrieval',
                         'ont:Query':'?shooting=Yes'
                        }
                    )
                doc.usage(get_Property2014, PropertyData2014_Resource, startTime, None,
                        {prov.model.PROV_TYPE:'ont:Retrieval',
                         'ont:Query':''
                        }
                    )
                doc.usage(get_Property2015, PropertyData2015_Resource, startTime, None,
                        {prov.model.PROV_TYPE:'ont:Retrieval',
                         'ont:Query':''
                        }
                    )
                doc.usage(get_Property2016, PropertyData2016_Resource, startTime, None,
                        {prov.model.PROV_TYPE:'ont:Retrieval',
                         'ont:Query':''
                        }
                    )
                doc.usage(get_PoliceLoc, PoliceLoc_Resource, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Retrieval',
                     'ont:Query':'?$select=location'
                    }
                )

                entertainmentLocation = doc.entity('dat:ll0406_siboz#entertainmentLocation', {prov.model.PROV_LABEL:'Entertainment Location', prov.model.PROV_TYPE:'ont:DataSet'})
                doc.wasAttributedTo(entertainmentLocation, this_script)
                doc.wasGeneratedBy(entertainmentLocation, get_Entertainment, endTime)
                doc.wasDerivedFrom(entertainmentLocation, Entertainment_Resource, get_Entertainment, get_Entertainment, get_Entertainment)

                crimeIncident = doc.entity('dat:ll0406_siboz#crimeIncident', {prov.model.PROV_LABEL:'2012-2015 Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
                doc.wasAttributedTo(crimeIncident, this_script)
                doc.wasGeneratedBy(crimeIncident, get_CrimeData, endTime)
                doc.wasDerivedFrom(crimeIncident, Crime_Resource, get_CrimeData, get_CrimeData, get_CrimeData)

                newCrimeIncident = doc.entity('dat:ll0406_siboz#newCrimeIncident', {prov.model.PROV_LABEL:'2015-2016 Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
                doc.wasAttributedTo(newCrimeIncident, this_script)
                doc.wasGeneratedBy(newCrimeIncident, get_NewCrimeData, endTime)
                doc.wasDerivedFrom(newCrimeIncident, Crime_Resource_New, get_NewCrimeData, get_NewCrimeData, get_NewCrimeData)

                policeLocation = doc.entity('dat:ll0406_siboz#policeLocation', {prov.model.PROV_LABEL:'Police Station Location', prov.model.PROV_TYPE:'ont:DataSet'})
                doc.wasAttributedTo(policeLocation, this_script)
                doc.wasGeneratedBy(policeLocation, get_PoliceLoc, endTime)
                doc.wasDerivedFrom(policeLocation, PoliceLoc_Resource, get_PoliceLoc, get_PoliceLoc, get_PoliceLoc)

                liquorLocation = doc.entity('dat:ll0406_siboz#liquorLocation', {prov.model.PROV_LABEL:'Liquor Store Data', prov.model.PROV_TYPE:'ont:DataSet'})
                doc.wasAttributedTo(liquorLocation, this_script)
                doc.wasGeneratedBy(liquorLocation, get_Liquor, endTime)
                doc.wasDerivedFrom(liquorLocation, Liquor_Resource, get_Liquor, get_Liquor, get_Liquor)

                foodLocation = doc.entity('dat:ll0406_siboz#foodLocation', {prov.model.PROV_LABEL:'Restaurant Data', prov.model.PROV_TYPE:'ont:DataSet'})
                doc.wasAttributedTo(foodLocation, this_script)
                doc.wasGeneratedBy(foodLocation, get_Food, endTime)
                doc.wasDerivedFrom(foodLocation, Food_Resource, get_Food, get_Food, get_Food)

                shootingCrimeIncident = doc.entity('dat:ll0406_siboz#shootingCrimeIncident', {prov.model.PROV_LABEL:'Shooting Crime Data', prov.model.PROV_TYPE:'ont:DataSet'})
                doc.wasAttributedTo(shootingCrimeIncident, this_script)
                doc.wasGeneratedBy(shootingCrimeIncident, get_ShootingCrime, endTime)
                doc.wasDerivedFrom(shootingCrimeIncident, Shooting_crime_Resource, get_ShootingCrime, get_ShootingCrime, get_ShootingCrime)

                propAssess2014 = doc.entity('dat:ll0406_siboz#propAssess2014', {prov.model.PROV_LABEL:'Property Assessment Data 2014', prov.model.PROV_TYPE:'ont:DataSet'})
                doc.wasAttributedTo(propAssess2014, this_script)
                doc.wasGeneratedBy(propAssess2014, get_Property2014, endTime)
                doc.wasDerivedFrom(propAssess2014, PropertyData2014_Resource, get_Property2014, get_Property2014, get_Property2014)

                propAssess2015 = doc.entity('dat:ll0406_siboz#propAssess2015', {prov.model.PROV_LABEL:'Property Assessment Data 2015', prov.model.PROV_TYPE:'ont:DataSet'})
                doc.wasAttributedTo(propAssess2015, this_script)
                doc.wasGeneratedBy(propAssess2015, get_Property2015, endTime)
                doc.wasDerivedFrom(propAssess2015, PropertyData2015_Resource, get_Property2015, get_Property2015, get_Property2015)

                propAssess2016 = doc.entity('dat:ll0406_siboz#propAssess2016', {prov.model.PROV_LABEL:'Property Assessment Data 2016', prov.model.PROV_TYPE:'ont:DataSet'})
                doc.wasAttributedTo(propAssess2016, this_script)
                doc.wasGeneratedBy(propAssess2016, get_Property2016, endTime)
                doc.wasDerivedFrom(propAssess2016, PropertyData2016_Resource, get_Property2016, get_Property2016, get_Property2016)


                repo.record(doc.serialize()) # Record the provenance document.
                repo.logout()

                return doc

dataRetrieval.execute()
doc = dataRetrieval.provenance()
open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof