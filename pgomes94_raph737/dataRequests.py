import requests, json,pprint, dml, prov.model

class dataRequests(dml.Algorithm):
	contributor = 'pgomes94_raph737'
	reads = []
	writes = ['pgomes94_raph737.dataRequests']

	def get_hospital_locations():
		hospital_data_request = requests.get("https://data.cityofboston.gov/resource/u6fv-m8v4.json")
		if hospital_data_request.status_code == 200:
			print("Successfully requested hopsital data.")
			hospital_locations = {}
			for json_data in hospital_data_request.json():
				hospital_locations[json_data['name']] = json_data['location']['coordinates']
			return hospital_locations
		else:
			print("Error: Hospital Request failed. Status code: {}".format(hospital_data_request.status_code))
			return -1

	def get_traffic_locations():
		traffic_data_request = requests.get("https://data.cityofboston.gov/resource/dih6-az4h.json")
		traffic_location_request = requests.get("https://data.cityofboston.gov/resource/3mu3-67d4.json")
		if traffic_data_request.status_code == 200 and traffic_location_request.status_code == 200:
			print("Successfully requested traffic location data.")
			id_to_location = {}
			traffic_locations = {}

			for json_data in traffic_location_request.json():
				traffic_id_to_location[json_data['uuid']] = (json_data['x'], json_data['y'])

			for json_data in traffic_data_request.json():
				traffic_locations[json_data['uuid']] = id_to_location[json_data['uuid']]
			return traffic_locations
		else:
			print("Error: Traffic Requests failed. Jam Data Status code: {} Point Data Status Code {}".format(traffic_data_request.status_code, traffic_location_request.status_code))
			return -1

	def get_crime_locations():
		crime_data_request = requests.get("https://data.cityofboston.gov/resource/29yf-ye7n.json")
		if crime_data_request.status_code == 200:
			print("Successfully requested crime location data.")
			crime_locations = {}
			for json_data in crime_data_request.json():
				crime_locations[json_data['incident_number']] = (json_data["long"], json_data["lat"])
			return crime_locations
		else:
			print("Error: Crime requests failed. Status code: {}".format(crime_data_request.status_code))
			return -1

	@staticmethod
	def execute(trial = False):
		#hospital_locations = get_hospital_locations()
		#traffic_locations = get_traffic_locations()
		#crime_locations = get_crime_locations()
		#return some_shit
		pass

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

locations = dataRequests.get_crime_locations()
print(len(locations))
