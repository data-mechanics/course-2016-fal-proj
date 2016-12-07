import json
import dml
import prov.model
import datetime
import uuid
import matplotlib.pyplot
import police_location_analysis
import numpy as np



class police_location_plot(dml.Algorithm):
	contributor = 'manda094_nwg_patels95'
	reads = ['manda094_nwg_patels95.police_locations_analysis']
	writes = []

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

		repo.logout()
		return doc

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

		f = open('police_locationMap.js', 'w')
		f.truncate()
		f.write("var police_location_json = {" + "\n" + "\"type\": \"FeatureCollection\"," + "\n" + "\n" + "\"features\": [" + "\n")

		f2 = open('kcluster_police_locationMap.js', 'w')
		f2.truncate()
		f2.write("var kcluster_police_location_json = {" + "\n" + "\"type\": \"FeatureCollection\"," + "\n" + "\n" + "\"features\": [" + "\n")


		#access all of the original location data points of the police stations
		og_police_locations = []
		og_police_locations = police_location_analysis.analysis_data[1]
		og_X = police_location_analysis.analysis_data[2]
		og_Y = police_location_analysis.analysis_data[3]
		XM = police_location_analysis.analysis_data[4]
		YM = police_location_analysis.analysis_data[5]
		k_cluster_M = police_location_analysis.analysis_data[6]

		ln = len(og_police_locations)
		lnX = len(k_cluster_M)

		# create geoJSON mapping with the coordinates of each crime in our dataset with a coordinate given
		count = 0
		for x,y in og_police_locations:
			f.write("{ \"type\": \"Feature\", \"geometry\": " + "{ \"type\": \"Point\", \"coordinates\":" + "[" + str(x) + "," + str(y) + "]" +  "}}")
			count = count + 1
			if (count == og_police_locations[ln-1]):
				f.write("\n")
			else:
				f.write("," + "\n")
		f.write("]" + "\n" + "}")

		count1 = 0
		for x,y in k_cluster_M:
			f2.write("{ \"type\": \"Feature\", \"geometry\": " + "{ \"type\": \"Point\", \"coordinates\":" + "[" + str(x) + "," + str(y) + "]" +  "}}")
			count1 = count1 + 1
			if (count1 == k_cluster_M[lnX-1]):
				f2.write("\n")
			else:
				f2.write("," + "\n")
		f2.write("]" + "\n" + "}")



		matplotlib.pyplot.scatter(og_X,og_Y)
		matplotlib.pyplot.show()

		matplotlib.pyplot.scatter(XM, YM)
		matplotlib.pyplot.show()

		repo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}


police_location_plot.execute()

## eof
