import dml
import prov.model
import uuid
import datetime
import json
import csv

class MBTAStops(dml.Algorithm):

	# Authenticate with MongoDB
	contributor = 'asanentz_ldebeasi_mshop_sinichol'
	reads = []
	writes = ['asanentz_ldebeasi_mshop_sinichol.mbta']

	@staticmethod
	def execute(trial = False):

		if trial:
			count = 0

		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("asanentz_ldebeasi_mshop_sinichol", "asanentz_ldebeasi_mshop_sinichol")

		repo.dropPermanent("mbta")
		repo.createPermanent("mbta")

		with open("source-data/stops.csv") as f:
			r = csv.reader(f)
			stops = list(r)

		for i in stops:
			title = i[2]
			lat = i[4]
			lng = i[5]
			res = repo.asanentz_ldebeasi_mshop_sinichol.mbta.insert_one({'name': title, 'latitude': lat, 'longitude': lng})
			if trial:
				count +=1
				if count > 80:
					break

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('asanentz_ldebeasi_mshop_sinichol', 'asanentz_ldebeasi_mshop_sinichol')

		# Provenance Data
		doc = prov.model.ProvDocument()
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:asanentz_ldebeasi_mshop_sinichol#mbta',
								{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
		resource = doc.entity('bdp:stops',
							  {'prov:label': 'MBTA Stops', prov.model.PROV_TYPE: 'ont:DataResource',
							   'ont:Extension': 'csv'})
		getMBTAStops = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(getMBTAStops, this_script)
		doc.usage(getMBTAStops, resource, startTime, None,
				  {prov.model.PROV_TYPE: 'ont:Retrieval'}
				  )

		mbtaData = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#mbta', {prov.model.PROV_LABEL: 'MBTA Stops',
																	   prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(mbtaData, this_script)
		doc.wasGeneratedBy(mbtaData, getMBTAStops, endTime)
		doc.wasDerivedFrom(mbtaData, resource, getMBTAStops, getMBTAStops, getMBTAStops)

		repo.record(doc.serialize())  # Record the provenance document.
		repo.logout()

		return doc


MBTAStops.execute()
doc = MBTAStops.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
