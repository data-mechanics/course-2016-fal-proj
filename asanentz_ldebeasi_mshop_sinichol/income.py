import dml
import prov.model
import uuid
import datetime
import json

class income(dml.Algorithm):

	# Authenticate with MongoDB
	contributor = 'asanentz_ldebeasi_mshop_sinichol'
	reads = ['asanentz_ldebeasi_mshop_sinichol.income']
	writes = ['asanentz_ldebeasi_mshop_sinichol.income']

	@staticmethod
	def execute(trial = False):

		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate("asanentz_ldebeasi_mshop_sinichol", "asanentz_ldebeasi_mshop_sinichol")

		repo.dropPermanent("income")
		repo.createPermanent("income")

		incomeData = {}
		file = open('source-data/Boston_IncomePerCapita.json').read()
		data = json.loads(file)

		for d in data[0]:
			income = data[0][d]['Per Capita Income']
			title = d.upper()
			incomeData[title] = income

		for item in incomeData:

			if item not in ['SOUGUS', 'ABINGTON', 'N SCITUATE', 'NORWOOD', 'ASHLAND', 'SAUGUS', 'WALTHAM', 'BROCKTON', 'MARSHFIELD', 'NORWOOD', 'MEDFORD', 'IPSWICH', 'COLLINS', 'PHIL', 'BOSTON MA 02']:
				res = repo.asanentz_ldebeasi_mshop_sinichol.income.insert_one(
					{'name': item, 'income': incomeData[item]})

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

		this_script = doc.agent('alg:asanentz_ldebeasi_mshop_sinichol#income',
								{prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
		resource = doc.entity('bdp:Boston_IncomePerCapita',
							  {'prov:label': 'Income Data', prov.model.PROV_TYPE: 'ont:DataResource',
							   'ont:Extension': 'json'})
		get_income = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_income, this_script)
		doc.usage(get_income, resource, startTime, None,
				  {prov.model.PROV_TYPE: 'ont:Retrieval'}
				  )

		income = doc.entity('dat:ldebeasi_mshop#income', {prov.model.PROV_LABEL: 'Income Data',
																	   prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(income, this_script)
		doc.wasGeneratedBy(income, get_income, endTime)
		doc.wasDerivedFrom(income, resource, get_income, get_income, get_income)

		repo.record(doc.serialize())  # Record the provenance document.
		repo.logout()

		return doc


income.execute()
doc = income.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
