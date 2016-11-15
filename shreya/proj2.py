import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import trends
import correlation
import linear_regression

class proj2(dml.Algorithm):
	contributor = 'shreya'
	reads = ['shreya.building_permits_kmeans','shreya.crime_reports_kmeans', 'shreya.earnings_kmeans']
	writes = []
	
	@staticmethod
	def execute(trial = True):
		startTime = datetime.datetime.now()
		#Database connection
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('shreya','shreya')

		def get_data(filename):
			data_list = [(2012,[]),(2013,[]),(2014,[]),(2015,[])]
			with open(filename,'r') as src:
				for line in src:
					line = line.split(',')
					try:
						year = int(line[0])
						if year == 2012:
							data_list[0][1] += [(float(line[1]),float(line[2]))]
						if year == 2013:
							data_list[1][1] += [(float(line[1]),float(line[2]))]
						if year == 2014:
							data_list[2][1] += [(float(line[1]),float(line[2]))]
						if year == 2015:
							data_list[3][1] += [(float(line[1]),float(line[2]))]
					except: continue
			return data_list

		buildings_data = get_data('buildings_means.csv')
		crimes_data = get_data('crimes_means.csv')
		earnings_data = get_data('earnings_means.csv')
		datasets = {'building_permits_kmeans':buildings_data, 'crime_reports_kmeans':crimes_data,'earnings_kmeans':earnings_data}
		for dataset in datasets:
			data2012 = datasets[dataset][0]
			data2013 = datasets[dataset][1]
			data2014 = datasets[dataset][2]
			data2015 = datasets[dataset][3]
			if trial:
				data2012 = (2012, data2012[1][0:3])
				data2013 = (2013, data2013[1][0:3])
				data2014 = (2014, data2014[1][0:3])
				data2015 = (2015, data2015[1][0:3])

			trends_results = trends.calculate(data2012,data2013,data2014,data2015)

			pscores = correlation.get_pscores(trends_results)
			repo.dropPermanent(dataset[0:-7]+"pscores")
			repo.createPermanent(dataset[0:-7]+"pscores")
			name = 'shreya.'+dataset[0:-7]+'pscores'
			repo[name].insert_one({dataset[0:-7]: pscores})

			linear_analysis = linear_regression.get_linear_regression(trends_results,trial)
			repo.dropPermanent(dataset[0:-7]+"linreg")
			repo.createPermanent(dataset[0:-7]+"linreg")
			repo['shreya.'+dataset[0:-7]+'linreg'].insert_one({dataset[0:-7]: linear_analysis})

		repo.logout()
		endTime = datetime.datetime.now()

		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('shreya', 'shreya')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:shreya#',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'],'ont:Extention':'py'})
		buildings_resource = doc.entity('dat:shreya/building_permits_kmeans', {'prov:label':'K-Means over 4 Years for Building Permits',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'csv'})
		crimes_resource = doc.entity('dat:shreya/crime_reports_kmeans',{'prov:label':'K-Means over 4 Years for Crime Reports',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'csv'})
		earnings_resource = doc.entity('dat:shreya/earnings_kmeans',{'prov:label':'K-means over 4 Years for Earnings',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'csv'})
		
		buildings_pscores = doc.entity('dat:shreya#buildings_pscores', {'prov:label':'P-Scores for Progression of Means over 4 Years for Building Permits',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'DataSet'})
		crimes_pscores = doc.entity('dat:shreya#crimes_pscores', {'prov:label':'P-Scores for Progression of Means over 4 Years for Crime Reports',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'DataSet'})
		earnings_pscores = doc.entity('dat:shreya#earnings_pscores', {'prov:label':'P-Scores for Progression of Means over 4 Years for Earnings',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'DataSet'})
		
		buildings_linreg = doc.entity('dat:shreya#buildings_linreg', {'prov:label':'Linear Regression for Progression of Means over 4 Years for Building Permits',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'DataSet'})
		crimes_linreg = doc.entity('dat:shreya#crimes_linreg', {'prov:label':'Linear Regression for Progression of Means over 4 Years for Crime Reports',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'DataSet'})
		earnings_linreg = doc.entity('dat:shreya#earnings_linreg', {'prov:label':'Linear Regression for Progression of Means over 4 Years for Earnings',prov.model.PROV_TYPE:'ont:DataResource','ont:Extension':'DataSet'})
		
		pscore_activity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:P-score Calculation'})
		linreg_activity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Linear Regression Calculation'})

		#Building Permits
		doc.wasAssociatedWith(pscore_activity, this_script)
		doc.wasAttributedTo(buildings_pscores, this_script)
		doc.wasGeneratedBy(buildings_pscores, pscore_activity, endTime)
		doc.wasDerivedFrom(buildings_pscores, buildings_resource, pscore_activity, pscore_activity, pscore_activity)

		doc.wasAssociatedWith(linreg_activity, this_script)
		doc.wasAttributedTo(buildings_linreg, this_script)
		doc.wasGeneratedBy(buildings_linreg, linreg_activity, endTime)
		doc.wasDerivedFrom(buildings_linreg, buildings_resource, linreg_activity, linreg_activity, linreg_activity)

		#Crime Reports
		doc.wasAttributedTo(buildings_pscores, this_script)
		doc.wasGeneratedBy(crimes_pscores, pscore_activity, endTime)
		doc.wasDerivedFrom(crimes_pscores, crimes_resource, pscore_activity, pscore_activity, pscore_activity)

		doc.wasAttributedTo(crimes_linreg, this_script)
		doc.wasGeneratedBy(crimes_linreg, linreg_activity, endTime)
		doc.wasDerivedFrom(crimes_linreg, crimes_resource, linreg_activity, linreg_activity, linreg_activity)

		#Earnings
		doc.wasAttributedTo(earnings_pscores, this_script)
		doc.wasGeneratedBy(earnings_pscores, pscore_activity, endTime)
		doc.wasDerivedFrom(earnings_pscores, earnings_resource, pscore_activity, pscore_activity, pscore_activity)

		doc.wasAttributedTo(earnings_linreg, this_script)
		doc.wasGeneratedBy(earnings_linreg, linreg_activity, endTime)
		doc.wasDerivedFrom(earnings_linreg, earnings_resource, linreg_activity, linreg_activity, linreg_activity)

		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()

		return doc
proj2.execute()