import requests, json,pprint,csv,dml, prov.model

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

	def get_police_station_locations():
		police_station_data = requests.get("https://data.cityofboston.gov/resource/pyxn-r3i2.json")
		if police_station_data.status_code == 200:
			print("Successfully requested police station locations")
			police_station_locations = {}
			for json_data in police_station_data.json():
				police_station_locations[json_data['name']] = (json_data['location']['coordinates'][0],json_data['location']['coordinates'][0])
		return police_station_locations

	def get_mbta_stops():
		stops = {}
		pp = pprint.PrettyPrinter(indent=4)

		api_key = '8xE5xPe7rESr2gzOaD06Pg'
		lat = "42.346961"
		lon = "-71.076640"
		request_string = "http://realtime.mbta.com/developer/api/v2/stopsbylocation?api_key="+api_key+"&lat="+lat+"&lon="+lon+"&format=json"
		mbta_stops = requests.get(request_string)

		data = mbta_stops.json()
		temp_stops = [(dat['stop_id'],dat['stop_name'],dat['stop_lat'],dat['stop_lon'])for dat in data['stop']]
		has_more_stops = True
		count = 0

		with open('mbtaStops.csv', 'w') as f:
			wr = csv.writer(f);
			while has_more_stops:

				if len(temp_stops) == 0:
					has_more_stops = False
					break

				for stop in temp_stops:
					print(len(temp_stops))
					if len(temp_stops) == 0:
						has_more_stops = False
						break

					if (stop[2],stop[3]) in stops:
						temp_stops.remove(stop)
					else:
						lat = stop[2]
						lon = stop[3]
						stops[(lat,lon)] = True
						new_dat = (stop[0],stop[1],lat,lon)

						wr.writerow(new_dat) 
						temp_stops.remove(new_dat)

						new_request = get_mbta_request_string(api_key,lat,lon)
						next_data = requests.get(new_request).json()['stop']
						new_temp_stops = [(dat['stop_id'],dat['stop_name'],dat['stop_lat'],dat['stop_lon']) for dat in next_data]
						for x in new_temp_stops:
							if (x[2],x[3]) not in stops.keys():
								temp_stops.append(x)
		print("Completed getting MBTA stops! Data can be found in mbtaStops.csv")
		return {}

	#Helper Function for MBTA querying
	def get_mbta_request_string(api_key,lat,lon):
		return "http://realtime.mbta.com/developer/api/v2/stopsbylocation?api_key="+api_key+"&lat="+lat+"&lon="+lon+"&format=json"


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


