import dml
import prov.model
import uuid
import datetime
import gpxpy.geo

contributor = "cyung20_kwleung"
reads = []
writes = ['cyung20_kwleung.liquorAndCrime']

client = dml.pymongo.MongoClient()
repo = client.repo 
repo.authenticate("cyung20_kwleung", "cyung20_kwleung") 

startTime = datetime.datetime.now()

repo.dropPermanent("liquorAndCrime")
repo.createPermanent("liquorAndCrime")

data = repo.cyung20_kwleung.crime.find()

#Consists of crime data we want before we use product()
crimeDetails = []
#counter and total makes sure only 10,000 crimes are 
#run through, so that our computer doesn't crash!
counter = 0
total = 10000
for d in data:
    theData = dict(d)
    if counter < 10000:
        try:
            lon = theData['long']
        except KeyError:
            lon = None
        try:
            lat = theData['lat']
        except KeyError:
            lat = None
        if lon != None and lat != None:
            crimeDetails += [['crime', [lon,lat]]]
    counter += 1

data = repo.cyung20_kwleung.liquor.find() 

#Consists of liquor store data we want before we use product()
liquorDetails = []

for d in data:
    theData = dict(d)
    businessName = theData['businessname'] 
    coord = theData['location']['coordinates'] 
    if coord[0] != 0 and coord[1] != 0:
        liquorDetails += [['liquor store', businessName, coord]]

def product(R, S):
        return [(t,u) for t in R for u in S]

def select(R, s):
    return [t for t in R if s(t)]

#Function used for select in order to "select" liquor stores within
#25 meters of a crime scene.
def dis(t):
    lon1 = t[0][2][0]
    lat1 = t[0][2][1]
    #print(lat1)
    lon2 = float(t[1][1][0])
    lat2 = float(t[1][1][1])
    #print(lat2)
    #dist calculates distance in meters
    dist = gpxpy.geo.haversine_distance(lat1, lon1, lat2, lon2)
    #If the distance is less or equal to 25 meters, keep it. 
    return dist <= 25

#Number of alcohol places at least 25 meters from crime.
#print(len(select(product(liquorDetails,crimeDetails),dis)))

#Alcohol places at least 25 meters from crime.
selected = select(product(liquorDetails,crimeDetails),dis)

instances = {}
#for x in range(0, len(selected)):
num = 1
for x in range(0, len(selected)):
    one = str(selected[x][0][1])
    two = str(selected[x][0][2])
    three = str(selected[x][1][1])
    instances["instance" + str(num)] = ['liquor', ['businessname', one],['location',two]]
    num += 1

#Insert data into liquorAndCrime
for d in instances:
    repo.cyung20_kwleung.liquorAndCrime.insert_one({d: instances[d]})

endTime = datetime.datetime.now()

doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/cyung20_kwleung') # The scripts are in <folder>#<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

this_script = doc.agent('alg:liquorAndCrime', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
crimeReports = doc.entity('dat:cyung20_kwleung#crime', {prov.model.PROV_LABEL:'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
liquorLicense = doc.entity('dat:cyung20_kwleung#liquor', {prov.model.PROV_LABEL:'Liquor Licenses', prov.model.PROV_TYPE:'ont:DataSet'})

this_final = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(this_final, this_script)
doc.used(this_final, liquorLicense, startTime)
doc.used(this_final, crimeReports, startTime)

crimeLiq = doc.entity('dat:liquorAndCrime', {prov.model.PROV_LABEL:'Proximity of crime locations and liquor', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(crimeLiq, this_script)
doc.wasGeneratedBy(crimeLiq, this_final, endTime)
doc.wasDerivedFrom(crimeLiq, crimeReports, this_final, this_final, this_final)
doc.wasDerivedFrom(crimeLiq, liquorLicense, this_final, this_final, this_final)

repo.record(doc.serialize())
repo.logout()

#print(repo.record(doc.serialize()))
print(doc.get_provn())

#Print out new data set!
'''data = repo.cyung20_kwleung.liquorAndCrime.find()

for d in data:
    print(dict(d))'''
