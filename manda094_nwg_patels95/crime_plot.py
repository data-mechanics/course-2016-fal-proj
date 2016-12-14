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


	@staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:manda094_nwg_patels95#crime_plot', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('dat:manda094_nwg_patels95#crimes', {'prov:label':'Crimes', prov.model.PROV_TYPE:'ont:DataResource'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        repo.record(doc.serialize())
        repo.logout()

        return doc
crime_plot.execute()

## eof
