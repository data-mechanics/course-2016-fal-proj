import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class Downloader(dml.Algorithm):
	contributor = 'jas91_smaf91'
	reads = []
	writes = ['jas91_smaf91.crime', 'jas91_smaf91.311', 'jas91_smaf91.hospitals', 'jas91_smaf91.food', 'jas91_smaf91.schools']

	@staticmethod
	def execute(trial = False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jas91_smaf91', 'jas91_smaf91')

		city_of_boston_datasets = {
			'crime': 'https://data.cityofboston.gov/resource/ufcx-3fdn.json',
			'sr311': 'https://data.cityofboston.gov/resource/rtbk-4hc4.json',
			'hospitals': 'https://data.cityofboston.gov/resource/u6fv-m8v4.json',
			'food': 'https://data.cityofboston.gov/resource/427a-3cn5.json',
			'schools': 'https://data.cityofboston.gov/resource/pzcy-jpz4.json'
		}

		for dataset in city_of_boston_datasets:
			response = urllib.request.urlopen(city_of_boston_datasets[dataset]).read().decode("utf-8")
			r = json.loads(response)
			s = json.dumps(r, sort_keys=True, indent=2)
			repo.dropPermanent(dataset)
			repo.createPermanent(dataset)
			repo['jas91_smaf91.' + dataset].insert_many(r)
			print('[OUT] Done loading dataset', dataset)

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

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
		repo.authenticate('alice_bob', 'alice_bob')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_found, this_script)
		doc.wasAssociatedWith(get_lost, this_script)
		doc.usage(get_found, resource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Retrieval',
					'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
					}
				)
		doc.usage(get_lost, resource, startTime, None,
				{prov.model.PROV_TYPE:'ont:Retrieval',
					'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
					}
				)

		lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(lost, this_script)
		doc.wasGeneratedBy(lost, get_lost, endTime)
		doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

		found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_found, endTime)
		doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()

		return doc

Downloader.execute()
'''
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
