import urllib.request
import json
from geopy.geocoders import Nominatim
exec(open('kmeans.py').read())

def earnings():
	#Converts zipcode to latitude, longitude coordinates.
	def json_from_url(url):
			response = urllib.request.urlopen(url).read().decode('utf-8')
			r = json.loads(response)
			return r
	def filter_earnings(record):
		newRecord = {}
		try:
			geolocator = Nominatim()
			location = geolocator.geocode(record['zip'])
			newRecord['location'] = [location.latitude, location.longitude]
			newRecord['total_earnings'] = float(record['total_earnings'])
			return newRecord
		except:
			return newRecord

	def process_earnings(earningsList):
		filtered = [filter_earnings(record) for record in earningsList]
		earnings = {}
		for record in filtered:
			if 'location' in record:
				location = tuple(record['location'])
				if location not in earnings:
					earnings[location] = record['total_earnings']
				else:
					earnings[location] += record['total_earnings']
		return kmeans(earnings)
	
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
	