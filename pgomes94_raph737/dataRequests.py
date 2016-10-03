import requests
import json
import pprint
import csv
import dml
import prov.model
import datetime
import urllib.request

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
		hospital_data_request = requests.get("https://data.cityofboston.gov/resource/u6fv-m8v4.json?$$app_token=" + dml.auth['city_of_boston'])
		if hospital_data_request.status_code == 200:
			print("Successfully requested hopsital data.")
			hospital_locations = {}
			for json_data in hospital_data_request.json():
				hospital_locations[json_data['name'].replace('.','')] = json_data['location']['coordinates']
			return hospital_locations
		else:
			print("Error: Hospital Request failed. Status code: {}".format(hospital_data_request.status_code))
			return -1

	def get_traffic_locations():
		traffic_data_request = requests.get("https://data.cityofboston.gov/resource/dih6-az4h.json?$$app_token=" + dml.auth['city_of_boston'])
		traffic_location_request = requests.get("https://data.cityofboston.gov/resource/3mu3-67d4.json?$$app_token=" + dml.auth['city_of_boston'])
		if traffic_data_request.status_code == 200 and traffic_location_request.status_code == 200:
			print("Successfully requested traffic location data.")
			id_to_location = {}
			traffic_locations = {}

			for json_data in traffic_location_request.json():
				id_to_location[json_data['uuid']] = (json_data['x'], json_data['y'])

			for json_data in traffic_data_request.json():
				try:
					traffic_locations[json_data['uuid']] = id_to_location[json_data['uuid']]
				except:
					pass
			return traffic_locations
		else:
			print("Error: Traffic Requests failed. Jam Data Status code: {} Point Data Status Code {}".format(traffic_data_request.status_code, traffic_location_request.status_code))
			return -1

	def get_crime_locations():
		crime_data_request = requests.get("https://data.cityofboston.gov/resource/29yf-ye7n.json?$$app_token=" + dml.auth['city_of_boston'])
		if crime_data_request.status_code == 200:
			print("Successfully requested crime location data.")
			crime_locations = {}
			for json_data in crime_data_request.json():
				try:
					crime_locations[json_data['incident_number']] = (json_data["long"], json_data["lat"])
				except:
					pass
			return crime_locations
		else:
			print("Error: Crime requests failed. Status code: {}".format(crime_data_request.status_code))
			return -1

	def get_police_station_locations():
		police_station_request = requests.get("https://data.cityofboston.gov/resource/pyxn-r3i2.json?$$app_token=" + dml.auth['city_of_boston'])
		if police_station_request.status_code == 200:
			print("Successfully requested police station locations")
			police_station_locations = {}
			for json_data in police_station_request.json():
				police_station_locations[json_data['name']] = (json_data['location']['coordinates'][0],json_data['location']['coordinates'][0])
			return police_station_locations
		else:
			print("Error: Police requests failed. Status code: {}".format(police_station_request.status_code))
			return -1

	def get_mbta_stop_locations_web():
		stops = {}
		pp = pprint.PrettyPrinter(indent=4)

		api_key = dml.auth['mbta']
		lat = "42.346961"
		lon = "-71.076640"
		request_string = get_mbta_request_string(api_key,lat,lon)
		mbta_stops = requests.get(request_string)

		data = mbta_stops.json()
		temp_stops = [(dat['stop_id'],dat['stop_name'],dat['stop_lat'],dat['stop_lon'])for dat in data['stop']]
		has_more_stops = True
		count = 0

		with open('./resources/mbtaStopsWeb.csv', 'w') as f:
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

	def get_mbta_stop_locations_csv():
		mbta_stops = {}
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
						mbta_stops[parts[0]] = {"stop_name":parts[2],"lat":parts[4],"lon":parts[5]}
					except:
						print("Error: Failed to parse line: {}".format(line))
			print("Successfully parsed MBTA stops csv.")
			return mbta_stops
		else:
			print("Error: Parsing MBTA stops csv failed with status code: {}.".format(mbta_stop_request.status_code))
			return -1

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('pgomes94_raph737', 'pgomes94_raph737')
		'''
		hospital_locations = dataRequests.get_hospital_locations()
		repo.dropPermanent("hospital_locations")
		repo.createPermanent("hospital_locations")
		repo['pgomes94_raph737.hospital_locations'].insert(hospital_locations)
		
		traffic_locations = dataRequests.get_traffic_locations()
		repo.dropPermanent("traffic_locations")
		repo.createPermanent("traffic_locations")
		repo['pgomes94_raph737.traffic_locations'].insert(traffic_locations)

		crime_locations = dataRequests.get_crime_locations()
		repo.dropPermanent("crime_locations")
		repo.createPermanent("crime_locations")
		repo['pgomes94_raph737.crime_locations'].insert(crime_locations)

		police_station_locations = dataRequests.get_police_station_locations()
		repo.dropPermanent("police_station_locations")
		repo.createPermanent("police_station_locations")
		repo['pgomes94_raph737.police_station_locations'].insert(police_station_locations)
		'''
		mbta_stop_locations = dataRequests.get_mbta_stop_locations_csv()
		repo.dropPermanent("mbta_stop_locations")
		repo.createPermanent("mbta_stop_locations")
		repo['pgomes94_raph737.mbta_stop_locations'].insert(mbta_stop_locations)
		
		repo.logout()

		endTime = datetime.datetime.now()

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass

dataRequests.execute()