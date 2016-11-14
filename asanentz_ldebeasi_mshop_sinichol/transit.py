import dml
import prov.model
import uuid
import datetime
import json

class transit(dml.Algorithm):
	#more like FUNctions haha
	#get it bc it's like FUN becomes a morpheme in function
	#but really function is its own free base
	#hoo boy comedy isn't dead
	#anyway these are from the CS591 lecture notes
	def union(R, S):
		return R + S
	def difference(R, S):
		return [t for t in R if t not in S]
	def intersect(R, S):
		return [t for t in R if t in S]
	def project (R, p):
		return [p(t) for t in R]
	def select(R, s):
		return [t for t in R if s(t)]
	def product(R, S):
		return [(t,u) for t in R for u in S]
	def aggregate(R, f):
		keys = {r[0] for r in R}
		return [(key, f([v for (k,v) in R if k == key])) for key in keys]

	contributor = "asanentz_ldebeasi_mshop_sinichol"
	reads = ["asanentz_ldebeasi_mshop_sinichol.transit"]
	writes = ["asanentz_ldebeasi_mshop_sinichol.transit"]

	@staticmethod
	def execute(trial = False):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("asanentz_ldebeasi_mshop_sinichol", "asanentz_ldebeasi_mshop_sinichol")

		startTime = datetime.datetime.now()

		repo.dropPermanent("transit")
		repo.createPermanent("transit")

		#a list of towns that are probably going to be in our data sets (i.e. not Lynn)
		#this is not conclusive, it's just easier to use
		#towns = ["BOSTON", "BROOKLINE", "CAMBRIDGE", "SOMERVILLE", "ALLSTON"]
		buses = repo.asanentz_ldebeasi_mshop_sinichol.busStops.find()
						
		for entry in buses:
			#making these data entries look nice

			#	if entry["properties"]["TOWN"] in towns:
			entry['properties']['TYPE'] = 'BUS'
			entry['properties']['LONGITUDE'] = entry['geometry']['coordinates'][0]
			entry['properties']['LATITUDE']  = entry['geometry']['coordinates'][1]

				#I've taken all meaningfull data from the geometry portion of entry
			res = repo.asanentz_ldebeasi_mshop_sinichol.transit.insert_one(entry['properties'])


		hubway = repo.asanentz_ldebeasi_mshop_sinichol.hubway.find()
		for entry in hubway:
			entry['properties']['TYPE'] = 'HUBWAY'
			#"simon," you say, "why are you removing and reinserting latitude and longitude?"
			#"because," i respond, "there is more information in the other lat and lon"

			#("but simon," you continue, "you're never gonna use that much of the coordinates."
			#"listen bub you can can it," i reply coolly)
			entry['properties']['LONGITUDE'] = entry['geometry']['coordinates'][0]
			entry['properties']['LATITUDE']  = entry['geometry']['coordinates'][1]
			del entry['properties']['long_']
			del entry['properties']['lat']
			del entry['properties']['lastCommWithServer'] #but honestly I have no idea what this number is or how to read it
			del entry['properties']['installDate']
			del entry['properties']['version'] #interesting that hubways have different versions, but ultimately irrelevant to a count
			del entry['properties']['removalDate'] #never NOT none, I think
			del entry['properties']['public_'] #why even have columns if they're all the same?
			del entry['properties']['lastUpdate']
			del entry['properties']['temporary_']

			res = repo.asanentz_ldebeasi_mshop_sinichol.transit.insert_one(entry['properties'])

		mbta = repo.asanentz_ldebeasi_mshop_sinichol.mbta.find()
		for t in mbta:
			t['TYPE'] = 'MBTA'
			t['LATITUDE'] = t['latitude']
			t['LONGITUDE'] = t['longitude']
			del t['latitude']
			del t['longitude']

			res = repo.asanentz_ldebeasi_mshop_sinichol.transit.insert_one(t)
			
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

		this_script = doc.agent('alg:combineBusAddress', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
		addresses = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#addresses', {prov.model.PROV_LABEL:'List of Addresses', prov.model.PROV_TYPE:'ont:DataSet'})
		busStops = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#busStops', {prov.model.PROV_LABEL:'List of Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})

		this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

		doc.wasAssociatedWith(this_run, this_script)
		doc.used(this_run, addresses, startTime)
		doc.used(this_run, busStops, startTime)

		# Our new combined data set
		maintenance = doc.entity('dat:transit', {prov.model.PROV_LABEL:'Number of bus stops near any address', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(maintenance, this_script)
		doc.wasGeneratedBy(maintenance, this_run, endTime)
		doc.wasDerivedFrom(maintenance, addresses, this_run, this_run, this_run)
		doc.wasDerivedFrom(maintenance, busStops, this_run, this_run, this_run)

		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()

		return doc


transit.execute()
doc = transit.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
