import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics
from bson.code import Code

class transformation4(dml.Algorithm):
	contributor = 'anuragp1_jl101995'
	reads = ['anuragp1_Jl101995.weather', 'anuragp1_jl101995.citibike']
	writes = ['anuragp1_jl101995.citibike_weather']

	@staticmethod
	def execute(Trial = False):
		'''Retrieve some datasets'''

		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

		weatherdata = repo.anuragp1_jl101995.weather.find()
		citibikedata = repo.anuragp1_jl101995.citibike.find()

		repo.dropPermanent('citibike_weather')
		repo.createPermanent('citibike_weather')

		# list of dictionary entries containing each day's {data, avg temp, precip}
		date_weather = []

		for w in weatherdata:
			int2string = str(int(w['DATE']))
			# Change weather date format from YYYYMMDD (int) => 'MM/DD/YYYY' (str) 
			datestring = int2string[4:6] + '/' + int2string[6:8] + '/' + int2string[0:4]

			# Calculate average temperature for the day
			avgtemp = statistics.mean([w['TMAX'], w['TMIN']])

			# Insert into our date
			insert_weather = {'Date': datestring, 'AvgTemp': avgtemp, 'Precip': w['PRCP']}
			date_weather.append(insert_weather)

		for c in citibikedata:
			for d in date_weather:
				# Change trip date format from "2014-01-01 00:16:13" (str) => 'MM/DD/YYYY' (str) 
				citibike_tripdate = c['starttime']
				citibike_tripdate = citibike_tripdate[5:7] + '/' + citibike_tripdate[8:10] + '/' + citibike_tripdate[0:4]

				if citibike_tripdate == d['Date']:
					cw_fields = {'Date':d['Date'], 'AvgTemp':d['AvgTemp'], 'Precip':d['Precip'], \
								'start_station_id': c['start station id'], 'start_station_name': c['start station name'], \
								'birthyear': c['birth year'], 'gender' : c['gender'], 'tripduration': c['tripduration'], \
								'start_station_long': c['start station longitude'], 'start_station_lat': c['start station latitude']}

					# insert dictionary into the database
					repo.anuragp1_jl101995.citibike_weather.insert_one(cw_fields)

		# end database connection
		repo.logout()

		endTime = datetime.datetime.now()

		return {"start": startTime, "end": endTime}

	@staticmethod
	def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
		'''
		Create the provenance document describing everything happening
		in this script. Each run of the script will generate a new
		document describing that invocation event.
		'''

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('mch', 'http://datamechanics.io/data/anuragp1_jl101995/')  # Data Mechanics S3 bucket (weather file source)
		doc.add_namespace('cbd', 'https://www.citibikenyc.com/system-data/') # CitiBike System Data 

		this_script = doc.agent('alg:anuragp1_jl101995#transformation4', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		weather_resource = doc.entity('mch:weather', {'prov:label':'NYC Weather Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		citibike_resource = doc.entity('cbd:tripdata', {'prov:label':'CitiBike Trip Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
		citibike_weather_resource = doc.entity('dat:turnstiles_and_weather', {'prov:label':'Turnstile and Weather Data', prov.model.PROV_TYPE:'ont:DataSet'})

		get_weather = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_citibike = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_citibike_weather = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

		doc.wasAssociatedWith(get_weather, this_script)
		doc.wasAssociatedWith(get_turnstile, this_script)
		doc.wasAssociatedWith(get_turnstile_weather, this_script)

		doc.usage(get_weather, weather_resource, startTime, None,
		          {prov.model.PROV_TYPE:'ont:DataSet'} )
		doc.usage(get_citibike, citibike_resource, startTime, None,
		          {prov.model.PROV_TYPE:'ont:DataSet'} )
		doc.usage(get_citibike_weather, citibike_weather_resource, startTime, None,
		          {prov.model.PROV_TYPE:'ont:Computation'} )

		weather = doc.entity('dat:anuragp1_jl101995#weather', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(weather, this_script)
		doc.wasGeneratedBy(weather, get_weather, endTime)
		doc.wasDerivedFrom(weather, weather_resource, get_weather, get_weather, get_weather)

		citibike = doc.entity('dat:anuragp1_jl101995#citibike', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(citibike, this_script)
		doc.wasGeneratedBy(citibike, get_citibike, endTime)
		doc.wasDerivedFrom(citibike, citibike_resource, get_citibike, get_citibike, get_citibike)

		citibike_weather = doc.entity('dat:anuragp1_jl101995#citibike_weather', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(citibike_weather, this_script)
		doc.wasGeneratedBy(citibike_weather, get_citibike_weather, endTime)
		doc.wasDerivedFrom(citibike_weather, turnstile_citibike_resource, get_citibike_weather, get_citibike_weather, get_citibike_weather)

		repo.record(doc.serialize())  # Record the provenance document.
		repo.logout()

		return doc


transformation4.execute()
doc = transformation4.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))






