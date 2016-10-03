import requests, json,pprint,csv

class dataRequests():
	def get_mbta_request_string(api_key,lat,lon):
		return "http://realtime.mbta.com/developer/api/v2/stopsbylocation?api_key="+api_key+"&lat="+lat+"&lon="+lon+"&format=json"

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
	print("Completed!")