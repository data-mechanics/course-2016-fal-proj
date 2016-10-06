import requests
import json
import pprint
import csv
import dml
import uuid
import prov.model
import datetime

class dataRequests(dml.Algorithm):
	contributor = 'pgomes94_raph737'
	reads = []
	writes = [
		'pgomes94_raph737.hospital_locations',
		'pgomes94_raph737.traffic_locations',
		'pgomes94_raph737.crime_locations',
		'pgomes94_raph737.police_station_locations',
		'pgomes94_raph737.mbta_stop_locations'
	]

	def get_hospital_locations():
		hospital_data_request = requests.get("https://data.cityofboston.gov/resource/u6fv-m8v4.json?$$app_token=" + dml.auth['services']['cityofbostondataportal']['token'])
		if hospital_data_request.status_code == 200:
			hospital_locations = []
			for json_data in hospital_data_request.json():
				hospital_locations.append({
					'identifier': json_data['name'].replace('.',''),
					'location': (json_data['location']['coordinates'][1],json_data['location']['coordinates'][0])
					})
			return hospital_locations
		else:
			print("Error: Hospital Request failed. Status code: {}".format(hospital_data_request.status_code))
			return -1

	def get_traffic_locations():
		traffic_data_request = requests.get("https://data.cityofboston.gov/resource/dih6-az4h.json?$$app_token=" + dml.auth['services']['cityofbostondataportal']['token'])
		traffic_location_request = requests.get("https://data.cityofboston.gov/resource/3mu3-67d4.json?$$app_token=" + dml.auth['services']['cityofbostondataportal']['token'])
		if traffic_data_request.status_code == 200 and traffic_location_request.status_code == 200:
			id_to_location = {}
			traffic_locations = []

			for json_data in traffic_location_request.json():
				id_to_location[json_data['uuid']] = (json_data['y'], json_data['x'])

			for json_data in traffic_data_request.json():
				try:
					traffic_locations.append({
						'identifier':json_data['uuid'],
						'location': id_to_location[json_data['uuid']]
						})
				except:
					pass
			return traffic_locations
		else:
			print("Error: Traffic Requests failed. Jam Data Status code: {} Point Data Status Code {}".format(traffic_data_request.status_code, traffic_location_request.status_code))
			return -1

	def get_crime_locations():
		crime_data_request = requests.get("https://data.cityofboston.gov/resource/29yf-ye7n.json?$$app_token=" + dml.auth['services']['cityofbostondataportal']['token'])
		if crime_data_request.status_code == 200:
			crime_locations = []
			for json_data in crime_data_request.json():
				try:
					crime_locations.append({
						'identifier':json_data['incident_number'],
						'location': (json_data["lat"], json_data["long"])
						})
				except:
					pass
			return crime_locations
		else:
			print("Error: Crime requests failed. Status code: {}".format(crime_data_request.status_code))
			return -1

	def get_police_station_locations():
		police_station_request = requests.get("https://data.cityofboston.gov/resource/pyxn-r3i2.json?$$app_token=" + dml.auth['services']['cityofbostondataportal']['token'])
		if police_station_request.status_code == 200:
			police_station_locations = []
			for json_data in police_station_request.json():
				police_station_locations.append({
					'identifier': json_data['name'],
					'location': (json_data['location']['coordinates'][1],json_data['location']['coordinates'][0])
					})
			return police_station_locations
		else:
			print("Error: Police requests failed. Status code: {}".format(police_station_request.status_code))
			return -1


	def get_mbta_stop_locations_csv():
		mbta_stops = []
		mbta_stop_request = requests.get("http://datamechanics.io/data/pgomes94_raph737/stops.csv")
		if mbta_stop_request.status_code == 200:
			first = True
			for line in mbta_stop_request.text.split('\n'):
				if first:
					first = False
				else:
					parts = line.split(',')
					if len(parts) == 1:
						# last line, end case
						continue
					try:
						mbta_stops.append({
							'identifier':parts[0],
							'location': (parts[4], parts[5])
							})
					except:
						print("Error: Failed to parse line: {}".format(line))
			return mbta_stops
		else:
			print("Error: Parsing MBTA stops csv failed with status code: {}.".format(mbta_stop_request.status_code))
			return -1

	@staticmethod
	def execute(trial = False):
		start_time = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('pgomes94_raph737', 'pgomes94_raph737')
		
		hospital_locations = dataRequests.get_hospital_locations()
		if hospital_locations == -1:
			return -1
		else:
			repo.dropPermanent("hospital_locations")
			repo.createPermanent("hospital_locations")
			repo['pgomes94_raph737.hospital_locations'].insert_many(hospital_locations)
			print("Database hospital_locations created!")
			
		traffic_locations = dataRequests.get_traffic_locations()
		if traffic_locations == -1:
			return -1
		else:
			repo.dropPermanent("traffic_locations")
			repo.createPermanent("traffic_locations")
			repo['pgomes94_raph737.traffic_locations'].insert_many(traffic_locations)
			print("Database traffic_locations created!")

		crime_locations = dataRequests.get_crime_locations()
		if crime_locations == -1:
			return -1
		else:
			repo.dropPermanent("crime_locations")
			repo.createPermanent("crime_locations")
			repo['pgomes94_raph737.crime_locations'].insert_many(crime_locations)
			print("Database crime_locations created!")

		police_station_locations = dataRequests.get_police_station_locations()
		if police_station_locations == -1:
			return -1
		else:
			repo.dropPermanent("police_station_locations")
			repo.createPermanent("police_station_locations")
			repo['pgomes94_raph737.police_station_locations'].insert_many(police_station_locations)
			print("Database police_station_locations created!")

		mbta_stop_locations = dataRequests.get_mbta_stop_locations_csv()
		if mbta_stop_locations == -1:
			return -1
		else:
			repo.dropPermanent("mbta_stop_locations")
			repo.createPermanent("mbta_stop_locations")
			repo['pgomes94_raph737.mbta_stop_locations'].insert_many(mbta_stop_locations)
			print("Database mbta_stop_locations created!")

		repo.logout()

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

		this_script = doc.agent('alg:pgomes94_raph737#dataRequests', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		hospital_locations_resource = doc.entity('bdp:u6fv-m8v4', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		traffic_locations_resource = doc.entity('bdp:dih6-az4h',{'prov:label':'Traffic Locations',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'json'})
		crime_locations_resource = doc.entity('bdp:29yf-ye7n',{'prov:label':'Crime Locations',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'json'})
		mbta_stops_resource = doc.entity('dat:pgomes94_raph737#stops',{'prov:label':'MBTA Stops',prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		police_stations_resource = doc.entity('bdp:pyxn-r3i2',{'prov:label':'Police Locations',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'json'})

		get_hospital_locations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_traffic_locations  = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_crime_locations    = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_mbta_stops         = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_police_stations    = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_hospital_locations, this_script)
		doc.wasAssociatedWith(get_traffic_locations, this_script)
		doc.wasAssociatedWith(get_crime_locations, this_script)
		doc.wasAssociatedWith(get_mbta_stops, this_script)
		doc.wasAssociatedWith(get_police_stations, this_script)

		doc.usage(get_hospital_locations,hospital_locations_resource,startTime,None,{prov.model.PROV_TYPE:'ont:Retrieval'})
		doc.usage(get_traffic_locations,traffic_locations_resource,startTime,None,{prov.model.PROV_TYPE:'ont:Retrieval'})
		doc.usage(get_crime_locations,crime_locations_resource,startTime,None,{prov.model.PROV_TYPE:'ont:Retrieval'})
		doc.usage(get_mbta_stops,mbta_stops_resource,startTime,None,{prov.model.PROV_TYPE:'ont:Retrieval'})
		doc.usage(get_police_stations,police_stations_resource,startTime,None,{prov.model.PROV_TYPE:'ont:Retrieval'})

		hospitals = doc.entity('dat:pgomes94_raph737#hospital_locations', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(hospitals, this_script)
		doc.wasGeneratedBy(hospitals, get_hospital_locations, endTime)
		doc.wasDerivedFrom(hospitals, hospital_locations_resource, get_hospital_locations)

		traffic = doc.entity('dat:pgomes94_raph737#traffic_locations', {prov.model.PROV_LABEL:'Traffic Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(traffic, this_script)
		doc.wasGeneratedBy(traffic, get_traffic_locations, endTime)
		doc.wasDerivedFrom(traffic, traffic_locations_resource, get_traffic_locations)

		crimes = doc.entity('dat:pgomes94_raph737#crime_locations', {prov.model.PROV_LABEL:'Crime Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(crimes, this_script)
		doc.wasGeneratedBy(crimes, get_crime_locations, endTime)
		doc.wasDerivedFrom(crimes, crime_locations_resource, get_crime_locations)

		police_stations = doc.entity('dat:pgomes94_raph737#police_station_locations', {prov.model.PROV_LABEL:'Police Station Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(police_stations, this_script)
		doc.wasGeneratedBy(police_stations, get_police_stations, endTime)
		doc.wasDerivedFrom(police_stations, police_stations_resource, get_police_stations)

		mbta_stops = doc.entity('dat:pgomes94_raph737#mbta_stop_locations', {prov.model.PROV_LABEL:'MBTA Stop Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(mbta_stops, this_script)
		doc.wasGeneratedBy(mbta_stops, get_mbta_stops, endTime)
		doc.wasDerivedFrom(mbta_stops, mbta_stops_resource, get_mbta_stops)

		repo.record(doc.serialize())
		repo.logout()

		return doc

dataRequests.execute()
doc = dataRequests.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof