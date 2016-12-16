import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getPropertyData(dml.Algorithm):
	contributor = 'jyaang_robinliu106'
	reads=[]
	writes=['jyaang_robinliu106.property', 'jyaang_robinliu106.found']

	@staticmethod
	def execute(trial=False):
		'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

		url = "https://data.cityofboston.gov/api/views/i7w8-ure5/rows.json?accessType=DOWNLOAD"
		response = urllib.request.urlopen(url).read().decode("utf-8")
		r = json.loads(response)
		s = json.dumps(r, sort_keys=True, indent=2)

		propertyData = r['data']

		# all of the land use types under LU (land use) key
		residentUse = ['CD', 'R1', 'R2', 'R3', 'R4', 'RC', 'RL']
		a = []
		for entry in propertyData:
			#if entry[26] == 0 or entry[-1]:
				#continue;
			#print(entry[-1], entry[-2])

			if entry[17] in residentUse:
				# We are getting the zipcode, coordinates, and value of every entry that has residence use
				if entry[26] == 0 or entry[-1] == "#N/A":
					continue
				else:
					a.append({"LU": entry[17], "coord": [entry[-2],entry[-1]], "value" : entry[26]})
		#print(a)
		repo.dropPermanent("property")
		repo.createPermanent("property")
		repo['jyaang_robinliu106.property'].insert_many(a)

		repo.logout()

		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
		doc.add_namespace('dat', 'http://datamechanics.io/data/')
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/')
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:jyaang_robinliu106#getPropertyData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:i7w8-ure5', {'prov:label':'Property Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_prop = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_prop, this_script)
		doc.usage(get_prop, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
		prop = doc.entity('dat:jyaang_robinliu106#property', {prov.model.PROV_LABEL:'Property Data', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(prop, this_script)
		doc.wasGeneratedBy(prop, get_prop, endTime)
		doc.wasDerivedFrom(prop, resource, get_prop, get_prop, get_prop)

		repo.record(doc.serialize())
		repo.logout()
		return doc

getPropertyData.execute()
doc = getPropertyData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
