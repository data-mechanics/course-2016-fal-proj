import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from geopy.geocoders import Nominatim

class proj1(dml.Algorithm):
	contributor = 'shreya'
	reads = []
	writes = ['shreya.building_permits_kmeans','shreya.crime_reports_kmeans', 'shreya.earnings_kmeans']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		#Database connection
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('admin','example')
		print("Logged into database")
		def json_from_url(url):
			response = urllib.request.urlopen(url).read().decode('utf-8')
			r = json.loads(response)
			return r

		def kmeans(points_with_weights):
			#Chose 45 means because that's approximately how many zipcodes Boston has.
			#This point is what is generated for the zipcode 02215.
			M = [(42.3457429616475,-71.1025665422545)]*45
			P = list(points_with_weights.keys())
			def dist(p,q):
				(x1,y1) = p
				(x2,y2) = q
				return (x1-x2)**2 + (y1-y2)**2

			def plus(args):
				p = [0,0]
				for (x,y) in args:
					p[0] +=x
					p[1] +=y
				return tuple(p)

			def scale(p,c):
				(x,y) = p
				return (x/c, y/c)

			def product(R, S):
				return [(t,u) for t in R for u in S]
			def aggregate(R, f):
				keys = {r[0] for r in R}
				return [(key, f([v for (k,v) in R if k == key])) for key in keys]

			count = 0
			OLD = []
			while ((sorted(OLD) != sorted(M)) and (count < 10)):
				OLD = M
				MPD = [(m, p, dist(m,p)) for (m,p) in product(M,P)]
				PDs = [(p, d) for (m,p,d) in MPD]
				PD = aggregate(PDs, min) 
				MP = [(m,p) for ((m,p,d),(p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
				MT = aggregate(MP, plus)
				#Instead of 1 put salaries as the weight.
				MWeight = [(m, points_with_weights[p]) for ((m, p, d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
				MC = aggregate(MWeight, sum)

				M = [scale(t,c) for ((m,t), (m2,c)) in product(MT, MC) if m==m2]
				M = sorted(M)
				count+=1
			return M
		#Approved building permits.
		#Has location in the form of coordinate points in the json object.
		print("Starting building permits")
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
		#Deleting variables no longer needed because was getting the error "Killed:9" which apparently means it's out of memory.
		del buildingPermits 
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

		for record in buildings_dates_subset:
			year = record['issued_date'][0:4]
			if year == "2012": building_permits_2012 = insertRecord(building_permits_2012, record)
			elif year == "2013": building_permits_2013 = insertRecord(building_permits_2013, record)
			elif year == "2014": building_permits_2014 = insertRecord(building_permits_2014, record)
			elif year == "2015": building_permits_2015 = insertRecord(building_permits_2015, record)

		print("Finished creating new Building Permits dataset with locations")
		print("Starting building permits k-means calculations")
		building_permits_2012 = kmeans(building_permits_2012)
		building_permits_2013 = kmeans(building_permits_2013)
		building_permits_2014 = kmeans(building_permits_2014)
		building_permits_2015 = kmeans(building_permits_2015)
		building_permits_kmeans = [(2012,building_permits_2012), (2013, building_permits_2013), (2014, building_permits_2014), (2015, building_permits_2015)]
		del building_permits_2012
		del building_permits_2013
		del building_permits_2014
		del building_permits_2015
		print("Finished Building Permits k-means")
		repo.dropPermanent("building_permits_kmeans")
		repo.createPermanent("building_permits_kmeans")
		repo['shreya.building_permits_kmeans'].insert_many(building_permits_kmeans)
		print("Inserted Building Permits in db")
		#Crime reports from July 2012-August 2015.
		crimeReports = json_from_url('https://data.cityofboston.gov/resource/ufcx-3fdn.json')
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
		print("Finished creating new Crime Reports dataset")
		print("Starting k-means calculations")
		crime2012 = kmeans(crime2012)
		crime2013 = kmeans(crime2013)
		crime2014 = kmeans(crime2014)
		crime2015 = kmeans(crime2015)
		crime_reports_kmeans = [(2012,crime2012), (2013,crime2013), (2014,crime2014), (2015, crime2015)]
		del crime2012
		del crime2013
		del crime2014
		del crime2015
		print("Finished Crime Reports k-means")
		repo.dropPermanent('crime_reports_kmeans')
		repo.createPermanent('crime_reports_kmeans')
		repo['shreya.crime_reports_kmeans'].insert_many(crime_reports_kmeans)
		print("Inserted Crime Reports in db")
		
		#Converts zipcode to latitude, longitude coordinates.
		def filter_earnings(record):
			newRecord = {}
			geolocator = Nominatim()
			location = geolocator.geocode(record['zip'])
			newRecord['location'] = [location.latitude, location.longitude]
			newRecord['total_earnings'] = int(record['total_earnings'])
			return newRecord

		def process_earnings(earningsList):
			filtered = [filter_earnings(record) for record in earningsList]
			earnings = {}
			for record in filtered:
				location = record['location']
				if location not in earnings:
					earnings[location] = record['total_earnings']
				else:
					earnings[location] += record['total_earnings']
			return kmeans(earnings)
		print("Earnings 2012")
		#Employee Earning Report 2012.
		earnings2012=process_earnings(json_from_url('https://data.cityofboston.gov/resource/wypd-uw2t.json'))
		print("Earnings 2013")
		#Employee Earning Report 2013.
		earnings2013= process_earnings(json_from_url('https://data.cityofboston.gov/resource/5kqy-n8eq.json'))
		print("Earnings 2014")
		#Employee Earning Report 2014.
		earnings2014 = process_earnings(json_from_url('https://data.cityofboston.gov/resource/ntv7-hwjm.json'))
		print("Earnings 2015")
		#Employee Earning Report 2015.
		earnings2015 = process_earnings(json_from_url('https://data.cityofboston.gov/resource/bejm-5s9g.json'))
		
		#Each tuple is the year and a list of 45 means representing the spread of wealth.
		#I've split them up by year so I can see the change in wealth alongside the other two datasets.
		all_earnings_kmeans = [(2012,earnings2012),(2013,earnings2013), (2014,earnings2014), (2015, earnings2015)]
		repo.dropPermanent('earnings_kmeans')
		repo.createPermanent('earnings_kmeans')
		repo['shreya.earnings_kmeans'].insert_many(all_earnings_kmeans)
		print("Inserted Earnings in db")
		repo.logout()
		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
         # Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('admin', 'example')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script - doc.agent('alg:shreya#', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #Building Permits
		resource = doc.entity('bdp:hfgw-p5wb',{'prov:label':'Approved Building Permits',prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_building_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_building_permits, this_script)
		doc.usage(get_building_permits, resource, startTime, None,
        			{prov.model.PROV_TYPE: 'ont:Retrieval'}) #idk what else to put here
		building_permits = doc.entity('dat:shreya#building_permits_kmeans',
        	{prov.model.PROV_LABEL:'K-Means of Approved Building Permits By Year (2012-2015)',
        	prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(building_permits, this_script)
		doc.wasGeneratedBy(building_permits, get_building_permits, endTime)
		doc.wasDerivedFrom(building_permits, resource, get_building_permits, get_building_permits, get_building_permits)

		#Crime Incident Reports
		resource = doc.entity('bdp:ufcx-3fdn',{'prov:label':'Crime Incident Reports',prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_crime_reports = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_crime_reports, this_script)
		doc.usage(get_crime_reports, resource, startTime, None,
        			{prov.model.PROV_TYPE: 'ont:Retrieval'}) #idk what else to put here
		crime_reports = doc.entity('dat:shreya#crime_reports_kmeans',
        	{prov.model.PROV_LABEL:'K-Means of Crime Reports By Year (2012-2015)',
        	prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(crime_reports, this_script)
		doc.wasGeneratedBy(crime_reports, get_crime_reports, endTime)
		doc.wasDerivedFrom(crime_reports, resource, get_crime_reports, get_crime_reports, get_crime_reports)

		#Employee Earnings Report
		resource2012 = doc.entity('bdp:wypd-uw2t',{'prov:label':'Crime Incident Reports',prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		resource2014 = doc.entity('bdp:ntv7-hwjm',{'prov:label':'Crime Incident Reports',prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		resource2013 = doc.entity('bdp:5kqy-n8eq',{'prov:label':'Crime Incident Reports',prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		resource2015 = doc.entity('bdp:bejm-5s9g',{'prov:label':'Crime Incident Reports',prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_earnings = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_earnings, this_script)
		doc.usage(get_earnings, resource, startTime, None,
        			{prov.model.PROV_TYPE: 'ont:Retrieval'}) #idk what else to put here. also how to put in multiple resources.
		earnings = doc.entity('dat:shreya#earnings_kmeans',
        	{prov.model.PROV_LABEL:'K-Means of Employee Earnings By Year (2012-2015)',
        	prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(earnings, this_script)
		doc.wasGeneratedBy(earnings, get_crime_reports, endTime)
		doc.wasDerivedFrom(earnings, resource, get_earnings, get_earnings, get_earnings)


		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()

		return doc

proj1.execute()

