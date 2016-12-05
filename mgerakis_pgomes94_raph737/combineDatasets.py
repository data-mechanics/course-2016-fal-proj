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

class combineDatasets(dml.Algorithm):
	contributor = 'mgerakis_pgomes94_raph737'
	reads = [
		'mgerakis_pgomes94_raph737.traffic_locations',
		'mgerakis_pgomes94_raph737.crime_locations',
		'mgerakis_pgomes94_raph737.police_station_locations',
		'mgerakis_pgomes94_raph737.mbta_stop_locations'
	]
	writes = [
		'mgerakis_pgomes94_raph737.proximity_locations',
		'mgerakis_pgomes94_raph737.proximity_cluster_centers'
	]

	def evaluate_clusters(X,max_clusters):
		error = np.zeros(max_clusters+1)
		error[0] = 0;
		for k in range(1,max_clusters+1):
			kmeans = KMeans(init='k-means++', n_clusters=k, n_init=10)
			kmeans.fit_predict(X)
			error[k] = kmeans.inertia_
		return error

	# prints the average estimated error of clustering a set of locations with [1-num_clusters] clusters
	def calculate_num_clusters(locations, num_clusters):
		average_errors = combineDatasets.evaluate_clusters(locations, num_clusters)
		for i in range(9):
			errors = combineDatasets.evaluate_clusters(locations, num_clusters)
			average_errors = np.add(average_errors, errors)
		average_errors /= 10.0
		for i in range(2,len(average_errors)):
			print((average_errors[i-1] - average_errors[i])/average_errors[i-1])

	@staticmethod
	def execute(trial = False):
		start_time = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('mgerakis_pgomes94_raph737', 'mgerakis_pgomes94_raph737')

		to_insert = []

		print("Reading in previous datasets.")
		
		# far from traffic spots, ambulances to move easier as they enter/leave hospital
		for vals in repo['mgerakis_pgomes94_raph737.traffic_locations'].find():
			to_insert.append({
				'origin': 'traffic_locations',
				'proximity': 'F',
				'location': vals['location']
			})

		# far from crime spots, how safe the area is
		for vals in repo['mgerakis_pgomes94_raph737.crime_locations'].find():
			to_insert.append({
				'origin': 'crime_locations',
				'proximity': 'F',
				'location': vals['location']
			})

		# close to police stations, safeness in emergencies
		for vals in repo['mgerakis_pgomes94_raph737.police_station_locations'].find():
			to_insert.append({
				'origin': 'police_locations',
				'proximity': 'C',
				'location': vals['location']
			})

		# close to mbta stops, accessibility to all people
		for vals in repo['mgerakis_pgomes94_raph737.mbta_stop_locations'].find():
			to_insert.append({
				'origin': 'mbta_locations',
				'proximity': 'C',
				'location': vals['location']
			})
			
		repo.dropPermanent("proximity_locations")
		repo.createPermanent("proximity_locations")
		repo['mgerakis_pgomes94_raph737.proximity_locations'].insert_many(to_insert)

		print("Database proximity_locations created!")
		print("Starting KMeans algorithm.")

		c_locations = [x['location'] for x in repo['mgerakis_pgomes94_raph737.proximity_locations'].find({'proximity': 'C'})]
		f_locations = [x['location'] for x in repo['mgerakis_pgomes94_raph737.proximity_locations'].find({'proximity': 'F'})]
		
		# looking at the estimated errors for labels 'C', chose to use 22 clusters
		#combineDatasets.calculate_num_clusters(c_locations, 25)

		# looking at the estimated errors for labels 'F', chose to use 23 clusters
		#combineDatasets.calculate_num_clusters(f_locations, 25)

		c_kmeans = KMeans(init='k-means++', n_clusters=22, max_iter=1000)
		f_kmeans = KMeans(init='k-means++', n_clusters=23, max_iter=1000)
		c_kmeans_centers = c_kmeans.fit(c_locations).cluster_centers_
		f_kmeans_centers = f_kmeans.fit(f_locations).cluster_centers_

		print("Kmeans centers calculated successfully!")

		to_insert = []
		for location in c_kmeans_centers:
			to_insert.append({
				'proximity' : 'C',
				'location' : location.tolist()
			})

		for location in f_kmeans_centers:
			to_insert.append({
				'proximity' : 'F',
				'location' : location.tolist()
			})

		repo.dropPermanent("proximity_cluster_centers")
		repo.createPermanent("proximity_cluster_centers")
		repo['mgerakis_pgomes94_raph737.proximity_cluster_centers'].insert_many(to_insert)

		print("Database proximity_cluster_centers created!")

		repo.logout()

		end_time = datetime.datetime.now()

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('mgerakis_pgomes94_raph737', 'mgerakis_pgomes94_raph737')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:mgerakis_pgomes94_raph737#combineDatasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		
		mbta_stops_resource = doc.entity('dat:mgerakis_pgomes94_raph737#mbta_stop_locations', {prov.model.PROV_LABEL:'MBTA Stop Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		crimes_resource = doc.entity('dat:mgerakis_pgomes94_raph737#crime_locations', {prov.model.PROV_LABEL:'Crime Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		traffic_resource = doc.entity('dat:mgerakis_pgomes94_raph737#traffic_locations', {prov.model.PROV_LABEL:'Traffic Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		police_stations_resource = doc.entity('dat:mgerakis_pgomes94_raph737#police_station_locations', {prov.model.PROV_LABEL:'Police Station Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		
		project_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(project_data, this_script)
		
		doc.usage(project_data, mbta_stops_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
		doc.usage(project_data, crimes_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
		doc.usage(project_data, traffic_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
		doc.usage(project_data, police_stations_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

		proximity_locations = doc.entity('dat:mgerakis_pgomes94_raph737#proximity_locations', {prov.model.PROV_LABEL:'Proximity of various locations that fall under different categories.', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(proximity_locations, this_script)
		doc.wasGeneratedBy(proximity_locations, project_data, endTime)
		doc.wasDerivedFrom(proximity_locations, mbta_stops_resource, project_data, project_data, project_data)
		doc.wasDerivedFrom(proximity_locations, crimes_resource, project_data, project_data, project_data)
		doc.wasDerivedFrom(proximity_locations, traffic_resource, project_data, project_data, project_data)
		doc.wasDerivedFrom(proximity_locations, police_stations_resource, project_data, project_data, project_data)
		
		cluster_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(cluster_data, this_script)
		
		doc.usage(cluster_data, proximity_locations, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
		
		proximity_cluster_locations = doc.entity('dat:mgerakis_pgomes94_raph737#proximity_cluster_locations', {prov.model.PROV_LABEL:'Clustered proximities of various locations that fall under different categories.', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(proximity_cluster_locations, this_script)
		doc.wasGeneratedBy(proximity_cluster_locations, cluster_data, endTime)
		doc.wasDerivedFrom(proximity_cluster_locations, proximity_locations, cluster_data, cluster_data, cluster_data)

		repo.record(doc.serialize())
		repo.logout()

		return doc

combineDatasets.execute()
doc = combineDatasets.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent = 4))

##eof
