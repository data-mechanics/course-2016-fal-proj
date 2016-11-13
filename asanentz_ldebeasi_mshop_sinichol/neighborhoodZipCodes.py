import dml
import prov.model
import uuid
import datetime
import json

class neighborhoodZipCodes(dml.Algorithm):

	# Authenticate with MongoDB
	contributor = 'asanentz_ldebeasi_mshop_sinichol'
	reads = ['asanentz_ldebeasi_mshop_sinichol.neighborhoods']
	writes = ['asanentz_ldebeasi_mshop_sinichol.neighborhoods']

	@staticmethod
	def execute(trial = False):

		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("asanentz_ldebeasi_mshop_sinichol", "asanentz_ldebeasi_mshop_sinichol")

		repo.dropPermanent("neighborhoods")
		repo.createPermanent("neighborhoods")

		# Get all Address
		data = repo.asanentz_ldebeasi_mshop_sinichol.addresses.find()
		neighborhoods = {}
		for address in data:
			toDict = dict(address)
			if toDict['owner'] == "CITY OF BOSTON":
				neighborhood = ' '.join(toDict['mail_cs'].split())[:-3]
				try:
					zip = toDict['zipcode']
				except KeyError:
					zip = toDict['mail_zipcode']

				# fix typos by boston
				if neighborhood in ["ROXBURY CROS", "ROXBURY CROSS"]:
					neighborhood = 'ROXBURY CROSSING'

				if neighborhood not in neighborhoods:
						neighborhoods[neighborhood] = {
							"zip":  [zip] ,
							"income":  None
						}
				else:
					if zip not in neighborhoods[neighborhood]["zip"]:
						neighborhoods[neighborhood]["zip"] += [zip]

		file = open('source-data/Boston_IncomePerCapita.json').read()
		data = json.loads(file)

		for d in data[0]:
			income = data[0][d]['Per Capita Income']
			title = d.upper()
			if title in neighborhoods:
				neighborhoods[title]["income"] = income


		for item in neighborhoods:
			if item not in ['SOUGUS', 'ABINGTON', 'N SCITUATE', 'NORWOOD', 'ASHLAND', 'SAUGUS', 'WALTHAM', 'BROCKTON', 'MARSHFIELD', 'NORWOOD', 'MEDFORD', 'IPSWICH', 'COLLINS', 'PHIL', 'BOSTON MA 02']:
				res = repo.asanentz_ldebeasi_mshop_sinichol.neighborhoods.insert_one( { 'name': item, 'zip': neighborhoods[item]['zip'], 'income': neighborhoods[item]['income'] })

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('asanentz_ldebeasi_mshop_sinichol', 'asanentz_ldebeasi_mshop_sinichol')

		# Provenance Data
		doc = prov.model.ProvDocument()
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/asanentz_ldebeasi_mshop_sinichol') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/asanentz_ldebeasi_mshop_sinichol') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:neighborhoods', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
		masterAddress = doc.entity('dat:asanentz_ldebeasi_mshop_sinichol#addresses', {prov.model.PROV_LABEL:'Master Address List', prov.model.PROV_TYPE:'ont:DataSet'})

		this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

		doc.wasAssociatedWith(this_run, this_script)
		doc.used(this_run, masterAddress, startTime)

		# Our new combined data set
		maintenance = doc.entity('dat:neighborhoods', {prov.model.PROV_LABEL:'All Neighborhoods with their list of zipcodes', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(maintenance, this_script)
		doc.wasGeneratedBy(maintenance, this_run, endTime)
		doc.wasDerivedFrom(maintenance, masterAddress, this_run, this_run, this_run)

		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()

		return doc


neighborhoodZipCodes.execute()
#doc = neighborhoodZipCodes.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
