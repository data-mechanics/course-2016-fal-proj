import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import matplotlib.pyplot


class crime_plot(dml.Algorithm):
	contributor = 'manda094_nwg_patels95'
	reads = ['manda094_nwg_patels95.crimes']
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

		f = open('crimeMap.js', 'w')
		f.truncate()
		f.write("var crimes_json = {" + "\n" + "\"type\": \"FeatureCollection\"," + "\n" + "\n" + "\"features\": [" + "\n")
		dt = []
		n = []
		e = []
		err = []

		# separates out all of the crimes that do not have a coordinate point
		for data in repo['manda094_nwg_patels95.crimes'].find():
			try:
				if not (data["location"]["coordinates"] == [0,0]):
					dt.append(data)
					n.append(data["location"]["coordinates"][1])
					e.append(data["location"]["coordinates"][0])
			except KeyError:
				err.append(data)
		ln = len(dt)

		# create geoJSON mapping with the coordinates of each crime in our dataset with a coordinate given
		for x in range(0, ln):
			dt[x]["geometry"] = dt[x].pop("location")
			f.write("{ \"type\": \"Feature\", \"geometry\": " + str(dt[x]["geometry"]) + "}")
			if (dt[x] == dt[ln-1]):
				f.write("\n")
			else:
				f.write("," + "\n")
		f.write("]" + "\n" + "}")

		matplotlib.pyplot.scatter(e,n)
		matplotlib.pyplot.show()

		repo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}
crime_plot.execute()

## eof