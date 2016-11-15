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

            this_script = doc.agent('alg:mgerakis_pgomes94_raph737#recommend', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
            proximity_cluster_centers = doc.entity('dat:mgerakis_pgomes94_raph737#proximity_cluster_centers', {'prov:label': 'Proximity Cluster Centers', prov.model.PROV_TYPE:'ont:DataSet'})
            
            cluster_close_centers = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(cluster_close_centers, this_script)
            
            doc.usage(cluster_close_centers, proximity_cluster_centers, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
            
            cluster_center = doc.entity('ont:kmeans_centers', {prov.model.PROV_LABEL:'Center of close cluster.', prov.model.PROV_TYPE:'ont:Computation'})
            doc.wasAttributedTo(cluster_center, this_script)
            doc.wasGeneratedBy(cluster_center, cluster_close_centers, endTime)
            doc.wasDerivedFrom(cluster_center, proximity_cluster_centers, cluster_close_centers, cluster_close_centers, cluster_close_centers)
            
            maximize_distance = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(maximize_distance, this_script)
            
            doc.usage(maximize_distance, cluster_center, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
            doc.usage(maximize_distance, proximity_cluster_centers, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
            
            optimal_coords = doc.entity('ont:optimal_coords', {prov.model.PROV_LABEL:'Optimal latitude, longtitude position', prov.model.PROV_TYPE:'ont:Computation'})
            doc.wasAttributedTo(optimal_coords, this_script)
            doc.wasGeneratedBy(optimal_coords, maximize_distance, endTime)
            doc.wasDerivedFrom(optimal_coords, cluster_center, maximize_distance, maximize_distance, maximize_distance)
            doc.wasDerivedFrom(optimal_coords, cluster_close_centers, maximize_distance, maximize_distance, maximize_distance)

            repo.record(doc.serialize())
            repo.logout()

            return doc


recommend.execute()
doc = recommend.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
