import dml
import prov.model
import uuid
import datetime
import json

class constraintSatisfaction(dml.Algorithm):
	contributor = "asanentz_ldebeasi_mshop_sinichol"
	reads = ["asanentz_ldebeasi_mshop_sinichol.addressValue"]
	writes = ["asanentz_ldebeasi_mshop_sinichol.constraintSatisfaction"]

	@staticmethod
	def execute(trial = False):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("asanentz_ldebeasi_mshop_sinichol", "asanentz_ldebeasi_mshop_sinichol")
		startTime = datetime.datetime.now()

		if trial:
			count = 0

		repo.dropPermanent("constraintSatisfaction")
		repo.createPermanent("constraintSatisfaction")

		values = repo.asanentz_ldebeasi_mshop_sinichol.addressValue.find()
		noTrans = {}
		noTransit = 0
		for value in values:
			tstops = value['T STOPS']
			buses = value['BUSES']
			hubway = value['HUBWAYS']

			

			if tstops == 0 and buses == 0 and hubway == 0:
				town = ' '.join(value['TOWN'].split()).upper() # gets rid of extraneous spaces
				if town[-2:] == 'MA': # we were getting some results in different states, so this weeds them out
					temp = dict()
					temp['ADDRESS'] = value['ADDRESS']
					temp['TOWN'] = town
					temp['ZIP'] = value['ZIP']
					temp['LAT'] = value['LATITUDE']
					temp['LONG'] = value['LONGITUDE']
					if town in noTrans:
						noTrans[town] += 1
					else:
						noTrans[town] = 1


					res = repo.asanentz_ldebeasi_mshop_sinichol.constraintSatisfaction.insert_one(temp)

					if trial:
						count += 1
						if count > 100:
							break

					noTransit += 1

		endTime = datetime.datetime.now()
		print(noTrans)
		if noTransit > 0:
			return {"success": False, "start": startTime, "end": endTime}
		else:
			return {"success": True, "start": startTime, "end": endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("asanentz_ldebeasi_mshop_sinichol", "asanentz_ldebeasi_mshop_sinichol")
		# Provenance Data
		doc = prov.model.ProvDocument()
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/')

		this_script = doc.agent('alg:asanentz_ldebeasi_mshop_sinichol#constraintSatisfaction', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
		addressValue = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#addressValue', {prov.model.PROV_LABEL:'Number of Bus, MBTA, and Hubway Stops near an address', prov.model.PROV_TYPE:'ont:DataSet'})
		

		this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

		doc.wasAssociatedWith(this_run, this_script)
		doc.used(this_run, addressValue, startTime)

		# Our new combined data set
		maintenance = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#constraintSatisfaction', {prov.model.PROV_LABEL:'Returns whether or not the constraint is satisfied', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(maintenance, this_script)
		doc.wasGeneratedBy(maintenance, this_run, endTime)
		doc.wasDerivedFrom(maintenance, addressValue, this_run, this_run, this_run)

		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()

		return doc

constraintSatisfaction.execute()
#doc = constraintSatisfaction.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

