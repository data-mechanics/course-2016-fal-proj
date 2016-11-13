import requests
import json
import pprint
import csv
import dml
import prov.model
import datetime
import uuid

from sklearn.cluster import KMeans
import numpy as np

from pyspark import SparkContext
from math import sqrt

def calculate_score(location, close_clusters, far_clusters):
	close_score = min([euclidean_distance(location, x) for x in close_clusters]) # closest cluster marked as close
	far_score = min([euclidean_distance(location, x) for x in far_clusters]) # closest cluster marked as far
	return far_score - close_score # maximize far, minimize close, want a positive score.

def euclidean_distance(x, y):
	return sqrt(pow(x[0] - y[0],2) + pow(x[1] - y[1],2))

class pysparkMapReduceJob(dml.Algorithm):
	contributor = 'pgomes94_raph737'
	reads = [
		'pgomes94_raph737.hospital_locations',
		'pgomes94_raph737.proximity_clusters'
	]
	writes = ['pgomes94_raph737.hospital_scores']

	@staticmethod
	def execute(trial = False):
		start_time = datetime.datetime.now()
		try:
			sc = SparkContext() # init pyspark with default configs

			client = dml.pymongo.MongoClient()
			repo = client.repo
			repo.authenticate('pgomes94_raph737', 'pgomes94_raph737')
			print('\n\n\n\n\n')
			print ("Starting MapReduce job.")
			close_clusters = [x['location'] for x in repo['pgomes94_raph737.proximity_cluster_centers'].find({'proximity': 'C'})]
			far_clusters = [x['location'] for x in repo['pgomes94_raph737.proximity_cluster_centers'].find({'proximity': 'F'})]

			hospital_locations_rdd = sc.parallelize([x for x in repo['pgomes94_raph737.hospital_locations'].find()])
			hospital_scores = hospital_locations_rdd.map(lambda x: {
							'identifier': x['identifier'],
							'score': calculate_score(x['location'], close_clusters, far_clusters)
				})
			print("Map reduce completed successfully!")

			repo.dropPermanent("hospital_scores")
			repo.createPermanent("hospital_scores")
			repo['pgomes94_raph737.hospital_scores'].insert_many(hospital_scores.collect())

			print("Database hospital_scores created.")
		except:
			print("Map reduce failed. Exiting now.")
		finally:
			repo.logout()
			sc.stop()

		end_time = datetime.datetime.now()

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('pgomes94_raph737', 'pgomes94_raph737')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:pgomes94_raph737#pysparkMapReduceJob', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		hospitals_resource = doc.entity('dat:pgomes94_raph737#hospital_locations', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		proximity_clusters_resources = doc.entity('dat:pgomes94_raph737#proximity_clusters', {prov.model.PROV_LABEL:'Proximity Clusters', prov.model.PROV_TYPE:'ont:DataSet'})

		get_hospital_locations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_proximity_clusters = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_hospital_scores = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_hospital_locations, this_script)
		doc.wasAssociatedWith(get_proximity_clusters, this_script)

		doc.usage(get_hospital_locations,hospitals_resource,startTime,None,{prov.model.PROV_TYPE:'ont:Retrieval'})
		doc.usage(get_proximity_clusters,proximity_clusters_resources,startTime,None,{prov.model.PROV_TYPE:'ont:Retrieval'})

		hospital_scores = doc.entity('dat:pgomes94_raph737#hospital_scores', {prov.model.PROV_LABEL:'Crime Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(hospital_scores,this_script)
		doc.wasGeneratedBy(hospital_scores,get_hospital_scores,endTime)
		doc.wasDerivedFrom(hospital_scores,hospitals_resource,proximity_clusters_resources,get_hospital_locations,get_proximity_clusters)

		repo.record(doc.serialize())
		repo.logout()

		return doc


if __name__ == "__main__":
	pysparkMapReduceJob.execute()
	doc = pysparkMapReduceJob.provenance()
	print(doc.get_provn())
	print(json.dumps(json.loads(doc.serialize()), indent=4))