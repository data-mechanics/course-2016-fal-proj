import requests
import json
import pprint
import csv
import dml
import prov.model
import datetime

class combineDatasets(dml.Algorithm):
	contributor = 'pgomes94_raph737'
	reads = [
		'pgomes94_raph737.traffic_locations',
		'pgomes94_raph737.crime_locations',
		'pgomes94_raph737.police_station_locations',
		'pgomes94_raph737.mbta_stop_locations'
	]
	writes = []

	@staticmethod
	def execute(trial = False):
		start_time = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('pgomes94_raph737', 'pgomes94_raph737')

		to_insert = []

		# far from traffic spots, ambulances to move easier as they enter/leave hospital
		for vals in repo['pgomes94_raph737.traffic_locations'].find():
			to_insert.append({
				'proximity': 'F',
				'location': vals['location']
				})

		# far from crime spots, how safe the area is
		for vals in repo['pgomes94_raph737.crime_locations'].find():
			to_insert.append({
				'proximity': 'F',
				'location': vals['location']
				})

		# close to police stations, safeness in emergencies
		for vals in repo['pgomes94_raph737.police_station_locations'].find():
			to_insert.append({
				'proximity': 'C',
				'location': vals['location']
				})

		# close to mbta stops, accessibility to all people
		for vals in repo['pgomes94_raph737.mbta_stop_locations'].find():
			to_insert.append({
				'proximity': 'C',
				'location': vals['location']
				})
			
		repo.dropPermanent("proximity_locations")
		repo.createPermanent("proximity_locations")
		repo['pgomes94_raph737.proximity_locations'].insert_many(to_insert)
		
		end_time = datetime.datetime.now()

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

combineDatasets.execute()