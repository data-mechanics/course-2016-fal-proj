import requests
import json
import pprint
import csv
import dml
import uuid
import prov.model
import datetime

class getError(dml.Algorithm):
	contributor = 'mgerakis_pgomes94_raph737'
	reads = ['mgerakis_pgomes94_raph737.hospital_scores']
	writes = []

	@staticmethod
	def execute(trial = False):
		start_time = datetime.datetime.now()

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('mgerakis_pgomes94_raph737', 'mgerakis_pgomes94_raph737')

		hospital_scores_list = []
		google_ratings = []

		print("Reading in hospital scores.")
		
		# get all the hospitals names and their scores that we calculated.
		for vals in repo['mgerakis_pgomes94_raph737.hospital_scores'].find():
			hospital_scores_list.append((vals['identifier'], vals['score']))
		hospital_scores_list = sorted(hospital_scores_list, key=lambda x: x[1], reverse=True)

		if trial == True:
			print ("Entering Trial mode. Only calculating for 5 hospitals.")
			hospital_scores_list = hospital_scores_list[0:5]

		print("Making requests to Googles maps api. NOTE THIS WILL RATE LIMIT QUICKLY IF RUN MULTIPLE TIMES.")

		google_base_url = "https://maps.googleapis.com/maps/api/place/textsearch/json?key={}&query=".format(dml.auth['services']['google']['token'])
		remove_indexes = []
		# skip hospitals we don't have rating info on and save indexes to remove
		for i in range(len(hospital_scores_list)):
			name,score = hospital_scores_list[i]
			try:
				url = google_base_url
				parts = name.split()
				for part in parts:
					url += part + '+'
				url = url[0:-1]
				crap = requests.get(url).json()['results'][0]['rating']
				google_ratings.append((name, crap))
			except:
				remove_indexes.append(i)

		# removes the indexes of the hospitals we dont have info on
		count = 0
		for i in remove_indexes:
			del hospital_scores_list[i-count]
			count += 1

		# calculates the error
		google_ratings = sorted(google_ratings, key=lambda x: x[1], reverse=True)
		total_error = 0.0
		for i in range(len(google_ratings)):
			name = google_ratings[i][0]
			for j in range(len(hospital_scores_list)):
				name2 = hospital_scores_list[j][0]
				if name == name2:
					total_error += abs(i-j)
					break

		average_error = total_error/len(google_ratings)
		print ('On average, our rating is {} spots off of the real data ordering.'.format(average_error))

		repo.logout()

		end_time = datetime.datetime.now()


	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('mgerakis_pgomes94_raph737', 'mgerakis_pgomes94_raph737')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:mgerakis_pgomes94_raph737#dataRequests', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		hospital_locations_resource = doc.entity('bdp:u6fv-m8v4', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		traffic_locations_resource = doc.entity('bdp:dih6-az4h',{'prov:label':'Traffic Locations',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'json'})
		crime_locations_resource = doc.entity('bdp:29yf-ye7n',{'prov:label':'Crime Locations',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'json'})
		mbta_stops_resource = doc.entity('dat:mgerakis_pgomes94_raph737#stops',{'prov:label':'MBTA Stops',prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
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

		hospitals = doc.entity('dat:mgerakis_pgomes94_raph737#hospital_locations', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(hospitals, this_script)
		doc.wasGeneratedBy(hospitals, get_hospital_locations, endTime)
		doc.wasDerivedFrom(hospitals, hospital_locations_resource)

		traffic = doc.entity('dat:mgerakis_pgomes94_raph737#traffic_locations', {prov.model.PROV_LABEL:'Traffic Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(traffic, this_script)
		doc.wasGeneratedBy(traffic, get_traffic_locations, endTime)
		doc.wasDerivedFrom(traffic, traffic_locations_resource)

		crimes = doc.entity('dat:mgerakis_pgomes94_raph737#crime_locations', {prov.model.PROV_LABEL:'Crime Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(crimes, this_script)
		doc.wasGeneratedBy(crimes, get_crime_locations, endTime)
		doc.wasDerivedFrom(crimes, crime_locations_resource)

		police_stations = doc.entity('dat:mgerakis_pgomes94_raph737#police_station_locations', {prov.model.PROV_LABEL:'Police Station Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(police_stations, this_script)
		doc.wasGeneratedBy(police_stations, get_police_stations, endTime)
		doc.wasDerivedFrom(police_stations, police_stations_resource)

		mbta_stops = doc.entity('dat:mgerakis_pgomes94_raph737#mbta_stop_locations', {prov.model.PROV_LABEL:'MBTA Stop Locations', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(mbta_stops, this_script)
		doc.wasGeneratedBy(mbta_stops, get_mbta_stops, endTime)
		doc.wasDerivedFrom(mbta_stops, mbta_stops_resource)

		repo.record(doc.serialize())
		repo.logout()

		return doc

getGoogleRatings.execute(trial=True)
#doc = dataRequests.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof