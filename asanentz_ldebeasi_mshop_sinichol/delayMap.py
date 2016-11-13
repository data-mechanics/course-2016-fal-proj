import dml
import prov.model
import uuid
import datetime
import json

class delayMap(dml.Algorithm):

	#from http://stackoverflow.com/questions/354038/how-do-i-check-if-a-string-is-a-number-float-in-python
	def is_number(s):
	    try:
	        float(s)
	        return True
	    except ValueError:
	        return False

	contributor = "asanentz_ldebeasi_mshop_sinichol"
	reads = ["asanentz_ldebeasi_mshop_sinichol.delayMap"]
	writes = ["asanentz_ldebeasi_mshop_sinichol.delayMap"]

	@staticmethod
	def execute(trial = False):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("asanentz_ldebeasi_mshop_sinichol", "asanentz_ldebeasi_mshop_sinichol")

		startTime = datetime.datetime.now()

		repo.dropPermanent("delayMap")
		repo.createPermanent("delayMap")


		delays = repo.asanentz_ldebeasi_mshop_sinichol.traffic.find()

		delayDict = {}

		#creates dictionary (key=street city, data=delay) of all delays in street, basically a simple mapreduce but
		#I don't understand mapreduce well so this is probably inefficient
		#I wish I had more practice with mapreduce

		#ALSO unrelated but they have Malcolm X Blvd as Malcolm 'X' Blvd which seems way problematic
		for entry in delays:
			if "street" in entry and "city" in entry:
				if "/" not in entry["street"]:
					addressKey = entry["street"].lower() + " " + entry["city"].lower().replace(",", "")
					if addressKey in delayDict: #this is the part I couldn't do in mapreduce quickly (i think it's a sum function like in class)
						delayDict[addressKey] += float(entry["delay"])
					else:
						delayDict[addressKey] = float(entry["delay"])
		#print(delayDict)
		addresses = repo.asanentz_ldebeasi_mshop_sinichol.addresses.find()

		count = 0
		for entry in addresses:

			#makes a useful data entry
			temp = dict() #just to take what is useful in the entry
			temp["ADDRESS"] = entry["mail_address"]
			temp["TOWN"] = entry["mail_cs"]
			temp["ZIP"] = entry["mail_zipcode"]
			temp["LATITUDE"] = entry["latitude"]
			temp["LONGITUDE"] = entry["longitude"]
			temp["TAX"] = entry["gross_tax"]


			#makes street into something like delayDict's
			street = entry["mail_address"].split()
			street = [x.lower() for x in street if x.isalpha()]
			street = " ".join(street)

			city = entry["mail_cs"].split()
			city = [x.lower() for x in city]
			#this messes up for San Jose, CA (oh no) but removes cardinal directions and is a quick fix but a bad line
			if len(city) == 3: city = city[1:]
			city = " ".join(city)

			addressKey = street + " " + city
			if addressKey in delayDict:
				temp["DELAY"] = delayDict[addressKey]
			else:
				temp["DELAY"] = "NO DELAY FOUND"


			res = repo.asanentz_ldebeasi_mshop_sinichol.delayMap.insert_one(temp)


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

		this_script = doc.agent('alg:delayMap', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
		addresses = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#addresses', {prov.model.PROV_LABEL:'List of Addresses', prov.model.PROV_TYPE:'ont:DataSet'})
		traffic = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#traffic', {prov.model.PROV_LABEL:'List of Delays by Streets', prov.model.PROV_TYPE:'ont:DataSet'})

		this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

		doc.wasAssociatedWith(this_run, this_script)
		doc.used(this_run, addresses, startTime)
		doc.used(this_run, traffic, startTime)

		# Our new combined data set
		maintenance = doc.entity('dat:delayMap', {prov.model.PROV_LABEL:'Delay by Address', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(maintenance, this_script)
		doc.wasGeneratedBy(maintenance, this_run, endTime)
		doc.wasDerivedFrom(maintenance, addresses, this_run, this_run, this_run)
		doc.wasDerivedFrom(maintenance, traffic, this_run, this_run, this_run)


		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()

		return doc

delayMap.execute()
doc = delayMap.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
