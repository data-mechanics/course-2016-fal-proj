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
            doc.add_namespace('goog', 'https://maps.googleapis.com/maps/api/') # Google API

            this_script = doc.agent('alg:mgerakis_pgomes94_raph737/getError', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
            hospital_scores_resource = doc.entity('dat:mgerakis_pgomes94_raph737#hospital_scores', {'prov:label':'Hospital Scores', prov.model.PROV_TYPE:'ont:DataSet'})
            hospital_google_reviews = doc.entity('goog:place', {'prov:label': 'Google Place Search', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
            
            calculate_error = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(calculate_error, this_script)
            
            doc.usage(calculate_error, hospital_scores_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
            doc.usage(calculate_error, hospital_google_reviews, startTime, None, {prov.model.PROV_TYPE:'ont:Query'})

            avg_error = doc.entity('ont:average_error', {prov.model.PROV_LABEL:'Average error of Google review scores and our Hospital scores', prov.model.PROV_TYPE:'ont:Computation'})
            doc.wasAttributedTo(avg_error, this_script)
            doc.wasGeneratedBy(avg_error, calculate_error, endTime)
            doc.wasDerivedFrom(avg_error, hospital_scores_resource, calculate_error, calculate_error, calculate_error)
            doc.wasDerivedFrom(avg_error, hospital_google_reviews, calculate_error, calculate_error, calculate_error)

            repo.record(doc.serialize())
            repo.logout()

            return doc

getError.execute(trial=True)
doc = getError.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
