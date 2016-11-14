import dml
import prov.model
import uuid
import datetime
import json


def is_number(s):
	try:
		float(s)
		return True
	except ValueError:
		return False

class addressValue(dml.Algorithm):
	#from http://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float-in-python


	contributor = "asanentz_ldebeasi_mshop_sinichol"
	reads = ["asanentz_ldebeasi_mshop_sinichol.addressValue"]
	writes = ["asanentz_ldebeasi_mshop_sinichol.addressValue"]

	@staticmethod
	def execute(trial = False):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("asanentz_ldebeasi_mshop_sinichol", "asanentz_ldebeasi_mshop_sinichol")
		startTime = datetime.datetime.now()

		repo.dropPermanent("addressValue")
		repo.createPermanent("addressValue")

		transit = repo.asanentz_ldebeasi_mshop_sinichol.transit.find()

		busStops = {} 
		tStops   = {}
		hubStops = {}
		chargeStops = {}



		for entry in transit:
			#essentially, this is creating a grid (coarser lon and lat) of stops and counting how many are 
			#in each square. I would much rather do this by zip code, but idk how to do that atm (geometry?)
			lat = round(float(entry['LATITUDE']), 2) #within about 1.1 km
			lon = round(float(entry['LONGITUDE']), 2)
			coords = (lat,lon)
			if entry["TYPE"] == "BUS":
				if coords in busStops:
					busStops[coords] += 1
				else:
					busStops[coords] = 1
			elif entry["TYPE"] == "HUBWAY":
				if coords in hubStops:
					hubStops[coords] += 1
				else:
					hubStops[coords] = 1
			elif entry['TYPE'] == 'MBTA':
				if coords in tStops:
					tStops[coords] += 1
				else:
					tStops[coords] = 1







		address = repo.asanentz_ldebeasi_mshop_sinichol.addresses.find()

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
				lat = round(float(entry['latitude']), 2)
				lon = round(float(entry['longitude']), 2)
				coords = (lat,lon)

				if coords in busStops:
					temp["BUSES"] = busStops[coords]
				else:
					temp["BUSES"] = 0
				if coords in hubStops:
					temp["HUBWAYS"] = hubStops[coords]
				else:
					temp["HUBWAYS"] = 0
				if coords in tStops:
					temp["T STOPS"] = tStops[coords]
				else:
					temp["T STOPS"] = 0
			else:
				temp["BUSES"] = "NO COORDS"
				temp["HUBWAYS"] = "NO COORDS"
				temp['T STOPS'] = "NO COORDS"

			res = repo.asanentz_ldebeasi_mshop_sinichol.addressValue.insert_one(temp)

		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("asanentz_ldebeasi_mshop_sinichol", "asanentz_ldebeasi_mshop_sinichol")
		# Provenance Data
		doc = prov.model.ProvDocument()
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/asanentz_ldebeasi_mshop_sinichol') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/asanentz_ldebeasi_mshop_sinichol') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/')

		this_script = doc.agent('alg:addressValue', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
		addresses = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#addresses', {prov.model.PROV_LABEL:'List of Addresses', prov.model.PROV_TYPE:'ont:DataSet'})
		busStops = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#busStops', {prov.model.PROV_LABEL:'List of Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
		hubway = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#hubway', {prov.model.PROV_LABEL:'List of Hubway Stops', prov.model.PROV_TYPE:'ont:DataSet'})
		tStops = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#mbta', {prov.model.PROV_LABEL:'List of MBTA Stops', prov.model.PROV_TYPE:'ont:DataSet'})


		this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

		doc.wasAssociatedWith(this_run, this_script)
		doc.used(this_run, addresses, startTime)
		doc.used(this_run, hubway, startTime)
		doc.used(this_run, tStops, startTime)

		# Our new combined data set
		maintenance = doc.entity('dat:addressValue', {prov.model.PROV_LABEL:'Number of Bus and Hubway Stops near an address', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(maintenance, this_script)
		doc.wasGeneratedBy(maintenance, this_run, endTime)
		doc.wasDerivedFrom(maintenance, addresses, this_run, this_run, this_run)
		doc.wasDerivedFrom(maintenance, busStops, this_run, this_run, this_run)
		doc.wasDerivedFrom(maintenance, hubway, this_run, this_run, this_run)
		doc.wasDerivedFrom(maintenance, tStops, this_run, this_run, this_run)

		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()

		return doc

addressValue.execute()
doc = addressValue.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

