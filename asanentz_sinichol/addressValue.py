import dml
import prov.model
import uuid
import datetime

#from http://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float-in-python
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

contributor = "asanentz_sinichol"
reads = []
writes = ["asanentz_sinichol.addressValue"]

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate("asanentz_sinichol", "asanentz_sinichol")

startTime = datetime.datetime.now()

repo.dropPermanent("addressValue")
repo.dropPermanent("addressValue")

transit = repo.asanentz_sinichol.transit.find()

busStops = {} 
tStops   = {}
hubStops = {}
chargeStops = {}



for entry in transit:
	#essentially, this is creating a grid (coarser lon and lat) of stops and counting how many are 
	#in each square. I would much rather do this by zip code, but idk how to do that atm (geometry?)
	lat = round(float(entry['LATITUDE']), 3) #within about 100 meters
	lon = round(float(entry['LONGITUDE']), 3)
	coords = (lat,lon)
	if entry["TYPE"] == "BUS":
		if coords in busStops:
			busStops[coords] += 1
		else:
			busStops[coords] = 1
	elif entry["TYPE"] == "T":
		if coords in tStops:
			tStops[coords] += 1
		else:
			tStops[coords] = 1
	elif entry["TYPE"] == "CHARGING_STATION":
		if coords in chargeStops:
			chargeStops[coords] += 1
		else:
			chargeStops[coords] = 1
	else:
		if coords in hubStops:
			hubStops[coords] += 1
		else:
			hubStops[coords] = 1






address = repo.asanentz_sinichol.addresses.find()

count = 0
for entry in address:
	temp = dict() #just to take what is useful in the entry
	temp["ADDRESS"] = entry["mail_address"]
	temp["TOWN"] = entry["mail_cs"]
	temp["ZIP"] = entry["mail_zipcode"]
	temp["LATITUDE"] = entry["latitude"]
	temp["LONGITUDE"] = entry["longitude"]
	temp["TAX"] = entry["gross_tax"]

	if is_number(temp["LATITUDE"]):
		lat = round(float(entry['latitude']), 3)
		lon = round(float(entry['longitude']), 3)
		coords = (lat,lon)

		if coords in busStops:
			temp["BUSES"] = busStops[coords]
		else:
			temp["BUSES"] = 0

		if coords in tStops:
			temp["TSTOPS"] = tStops[coords]
		else:
			temp["TSTOPS"] = 0

		if coords in hubStops:
			temp["HUBWAYS"] = hubStops[coords]
		else:
			temp["HUBWAYS"] = 0

		if coords in chargeStops:
			temp["HUBWAYS"] = hubStops[coords]
		else:
			temp["HUBWAYS"] = 0
	else:
		temp["BUSES"] = "NO COORDS"
		temp["TS"] = "NO COORDS"
		temp["HUBWAYS"] = "NO COORDS"

	res = repo.asanentz_sinichol.combineHubwayAddress.insert_one(temp)

endTime = datetime.datetime.now()


# Provenance Data
doc = prov.model.ProvDocument()
doc.add_namespace('alg', 'http://datamechanics.io/algorithm/asanentz_sinichol') # The scripts are in <folder>#<filename> format.
doc.add_namespace('dat', 'http://datamechanics.io/data/asanentz_sinichol') # The data sets are in <user>#<collection> format.
doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/')

this_script = doc.agent('alg:addressValue', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
addresses = doc.entity('dat:asanentz_sinichol#addresses', {prov.model.PROV_LABEL:'List of Addresses', prov.model.PROV_TYPE:'ont:DataSet'})
busStops = doc.entity('dat:asanentz_sinichol#busStops', {prov.model.PROV_LABEL:'List of Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
hubway = doc.entity('dat:asanentz_sinichol#hubway', {prov.model.PROV_LABEL:'List of Hubway Stops', prov.model.PROV_TYPE:'ont:DataSet'})

this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

doc.wasAssociatedWith(this_run, this_script)
doc.used(this_run, addresses, startTime)
doc.used(this_run, hubway, startTime)

# Our new combined data set
maintenance = doc.entity('dat:addressValue', {prov.model.PROV_LABEL:'Number of Bus and Hubway Stops near an address', prov.model.PROV_TYPE:'ont:DataSet'})
doc.wasAttributedTo(maintenance, this_script)
doc.wasGeneratedBy(maintenance, this_run, endTime)
doc.wasDerivedFrom(maintenance, addresses, this_run, this_run, this_run)
doc.wasDerivedFrom(maintenance, busStops, this_run, this_run, this_run)
doc.wasDerivedFrom(maintenance, hubway, this_run, this_run, this_run)

