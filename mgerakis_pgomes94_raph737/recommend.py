import requests
import json
import pprint
import csv
import dml
import uuid
import prov.model
import datetime

from sklearn.cluster import KMeans
import numpy as np

class recommend(dml.Algorithm):
	contributor = 'mgerakis_pgomes94_raph737'
	reads = ['mgerakis_pgomes94_raph737.proximity_cluster_centers']
	writes = []

	@staticmethod
	def execute(trial = False):
		start_time = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('mgerakis_pgomes94_raph737', 'mgerakis_pgomes94_raph737')

		close_clusters = [x['location'] for x in repo['mgerakis_pgomes94_raph737.proximity_cluster_centers'].find({'proximity': 'C'})]
		far_clusters = [x['location'] for x in repo['mgerakis_pgomes94_raph737.proximity_cluster_centers'].find({'proximity': 'F'})]

		if trial == True:
			close_clusters = close_clusters[0:5]
			far_clusters = far_clusters[0:5]

		kmeans = KMeans(init='k-means++', n_clusters=5, max_iter=1000)
		kmeans_centers = kmeans.fit(close_clusters).cluster_centers_

		# calculate distance to all the far clusters
		distances  = []
		for x,y in kmeans_centers:
			distance = 0.0
			for i,j in far_clusters:
				distance += pow(pow(x-i,2) + pow(y-j,2), .5)
			distances.append(distance)

		# gets the index which maximizes the distance to all the far clusters
		val = 0.0
		index = -1
		for i in range(len(distances)):
			if val < distances[i]:
				val = distances[i]
				index = i

		print ('The optimal lat/long is {}, {}'.format(kmeans_centers[index][0], kmeans_centers[index][1]))

		repo.logout()

		end_time = datetime.datetime.now()


	@staticmethod
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('mgerakis_pgomes94_raph737', 'mgerakis_pgomes94_raph737')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:mgerakis_pgomes94_raph737#recommend.py', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		optimal_hospital_location_resource = doc.entity('bdp:u6fv-m8v4', {'prov:label':'Optimal Hospital Location', prov.model.PROV_TYPE:'ont:Computation'})

		get_optimal_hospital_location = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_optimal_hospital_location, this_script)

		doc.usage(get_optimal_hospital_location,optimal_hospital_location_resource,startTime,None,{prov.model.PROV_TYPE:'ont:Retrieval'})

		hospitalScores = doc.entity('dat:mgerakis_pgomes94_raph737#optimal_hospital_location', {prov.model.PROV_LABEL:'Hospital Scores', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(optimal_hospital_location_resource, this_script)
		doc.wasGeneratedBy(optimal_hospital_location_resource, get_optimal_hospital_location, endTime)
		doc.wasDerivedFrom(optimal_hospital_location_resource)

		repo.record(doc.serialize())
		repo.logout()

		return doc


recommend.execute()
#doc = dataRequests.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof