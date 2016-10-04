import urllib.request
import json
from geopy.geocoders import Nominatim
exec(open('kmeans.py').read())

def earnings():
	#Converts zipcode to latitude, longitude coordinates.
	def json_from_url(url):
			response = urllib.request.urlopen(url).read().decode('utf-8')
			r = json.loads(response)
			#TAKE THIS OUT. JUST FOR TESTING
			return r[0:50]

	def group_by_zip(earningsList):
		zip_groups = {}
		for record in earningsList:
			if 'zip' not in record:
				continue
			zip = record['zip']
			if zip not in zip_groups:
				zip_groups[zip] = []
			zip_groups[zip] += [float(record['total_earnings'])]
	
		for zip in zip_groups:
			zip_groups[zip] = sum(zip_groups[zip])/len(zip_groups[zip])
		return zip_groups

	def zip_to_coordinates(zip_groups):
		coordinates_with_weights = {}
		for zip in zip_groups:
			try:
				geolocator = Nominatim()
				location = geolocator.geocode(zip)
				location = tuple([location.latitude, location.longitude])
				coordinates_with_weights[location] = zip_groups[zip]
			except:
				continue
		return coordinates_with_weights	

	def process_earnings(earningsList):
		#Dictionary of keys zipcodes to values of average income in that zipcode.
		zip_groups = group_by_zip(earningsList)
		coordinates_with_weights = zip_to_coordinates(zip_groups)
		return kmeans(coordinates_with_weights)
	
	#Employee Earning Report 2012.
	earnings2012=process_earnings(json_from_url('https://data.cityofboston.gov/resource/wypd-uw2t.json'))

	#Employee Earning Report 2013.
	earnings2013= process_earnings(json_from_url('https://data.cityofboston.gov/resource/5kqy-n8eq.json'))

	#Employee Earning Report 2014.
	earnings2014 = process_earnings(json_from_url('https://data.cityofboston.gov/resource/ntv7-hwjm.json'))

	#Employee Earning Report 2015.
	earnings2015 = process_earnings(json_from_url('https://data.cityofboston.gov/resource/bejm-5s9g.json'))
		
	#Each tuple is the year and a list of 45 means representing the spread of wealth.
	#I've split them up by year so I can see the change in wealth alongside the other two datasets.
	all_earnings_kmeans = [(2012,earnings2012),(2013,earnings2013), (2014,earnings2014), (2015, earnings2015)]
	return all_earnings_kmeans
	