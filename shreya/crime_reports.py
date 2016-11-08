import urllib.request
import json
exec(open('kmeans.py').read())

def crime_reports():
	url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json'
	response = urllib.request.urlopen(url).read().decode('utf-8')
	crimeReports = json.loads(response)
	crime2012 = {}
	crime2013 = {}
	crime2014 = {}
	crime2015 = {}
	for record in crimeReports:
		year = record['fromdate'][0:4]
		if year == "2012":
			crime2012[tuple(record['location']['coordinates'] )] = 1
		elif year == "2013":
			crime2013[tuple(record['location']['coordinates'] )] = 1
		elif year == "2014":
			crime2014[tuple(record['location']['coordinates'] )] = 1
		elif year == "2015":
			crime2015[tuple(record['location']['coordinates'] )] = 1
	crime2012 = kmeans(crime2012)
	crime2013 = kmeans(crime2013)
	crime2014 = kmeans(crime2014)
	crime2015 = kmeans(crime2015)
	crime_reports_kmeans = [(2012,crime2012), (2013,crime2013), (2014,crime2014), (2015, crime2015)]
	#Because was running out of RAM.
	del crime2012
	del crime2013
	del crime2014
	del crime2015
	return crime_reports_kmeans
	