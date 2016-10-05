import requests
import json
import pprint
import csv
import dml
import prov.model
import datetime

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
		pass

if __name__ == "__main__":
	pysparkMapReduceJob.execute()