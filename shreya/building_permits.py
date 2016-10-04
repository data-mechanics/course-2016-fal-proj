import json
import urllib.request
from geopy.geocoders import Nominatim
exec(open('kmeans.py').read())

def building_permits():
	#Approved building permits.
	#Has location in the form of coordinate points in the json object.
	def json_from_url(url):
		response = urllib.request.urlopen(url).read().decode('utf-8')
		r = json.loads(response)
		return r
	
	buildingPermits = json_from_url('https://data.cityofboston.gov/resource/hfgw-p5wb.json')
	#Variation on the map function given in class.
	#Ensures that dates are between July 2012 and August 2015, to match the Crime Incident Reports.
	def filter_dates(record):
		year = int(record['issued_date'][0:4])
		if year > 2011 and year < 2016:
			month = int(record['issued_date'][5:7])
			if year == 2012 and month < 7: return False
			if year == 2015 and month > 8: return False
			else:
				return True
	buildings_dates_subset = [record for record in buildingPermits if filter_dates(record)]
	buildings_permits_not_expired = [record for record in buildings_dates_subset if record['status']!='EXPIRED']
	#Deleting variables no longer needed because was getting the error "Killed:9" which apparently means it's out of memory.
	del buildingPermits 
	del buildings_dates_subset
	
	building_permits_2012 = {}
	building_permits_2013 = {}
	building_permits_2014 = {}
	building_permits_2015 = {}
		
	def insertRecord(building_permits_dictionary, record):
		#Gecoder was having ServiceErrors, so had to implement this.
		try:
			geolocator = Nominatim()
			if 'location' not in record:
				if 'zip' in record:
					location = geolocator.geocode(record['zip'])
					if (location.latitude, location.longitude) in building_permits_dictionary:
						building_permits_dictionary[(location.latitude, location.longitude)] += 1
					else:
						building_permits_dictionary[(location.latitude, location.longitude)] = 1
			else:
				building_permits_dictionary[tuple(record['location']['coordinates'])] = 1
			return building_permits_dictionary
		except:
			return building_permits_dictionary
		
	for record in buildings_permits_not_expired:
		year = record['issued_date'][0:4]
		if year == "2012": building_permits_2012 = insertRecord(building_permits_2012, record)
		elif year == "2013": building_permits_2013 = insertRecord(building_permits_2013, record)
		elif year == "2014": building_permits_2014 = insertRecord(building_permits_2014, record)
		elif year == "2015": building_permits_2015 = insertRecord(building_permits_2015, record)
	del buildings_permits_not_expired

	building_permits_kmeans = []
	
	building_permits_2012 = kmeans(building_permits_2012)
	building_permits_kmeans += [(2012,building_permits_2012)]
	del building_permits_2012
	
	building_permits_2013 = kmeans(building_permits_2013)
	building_permits_kmeans += [(2013, building_permits_2013)]
	del building_permits_2013
	
	building_permits_2014 = kmeans(building_permits_2014)
	building_permits_kmeans += [(2014, building_permits_2014)]
	del building_permits_2014
	
	building_permits_2015 = kmeans(building_permits_2015)
	building_permits_kmeans += [(2015,building_permits_2015)]
	del building_permits_2015

	return building_permits_kmeans
	