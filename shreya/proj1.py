import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import building_permits
import crime_reports
import earnings

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
		repo.authenticate('shreya','shreya')
		
		building_permits_kmeans = building_permits.building_permits()
		repo.dropPermanent("building_permits_kmeans")
		repo.createPermanent("building_permits_kmeans")
		repo['shreya.building_permits_kmeans'].insert_many([{'a':123}])
		
		crime_reports_kmeans = crime_reports.crime_reports()
		repo.dropPermanent('crime_reports_kmeans')
		repo.createPermanent('crime_reports_kmeans')
		repo['shreya.crime_reports_kmeans'].insert_many(crime_reports_kmeans)

		all_earnings_kmeans = earnings.earnings()
		repo.dropPermanent('earnings_kmeans')
		repo.createPermanent('earnings_kmeans')
		repo['shreya.earnings_kmeans'].insert_many(all_earnings_kmeans)

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

		this_script = doc.agent('alg:shreya#', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        #Building Permits
		resource = doc.entity('bdp:hfgw-p5wb',{'prov:label':'Approved Building Permits',prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_building_permits = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_building_permits, this_script)
		doc.usage(get_building_permits, resource, startTime, None,
        			{prov.model.PROV_TYPE: 'ont:Retrieval','ont:Computation':'k-means'}) 
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
        			{prov.model.PROV_TYPE: 'ont:Retrieval','ont:Computation':'k-means'}) 
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
		doc.usage(get_earnings, resource2012, startTime, None,
        			{prov.model.PROV_TYPE: 'ont:Retrieval','ont:Computation':'k-means'}) 
		doc.usage(get_earnings, resource2013, startTime, None,
        			{prov.model.PROV_TYPE: 'ont:Retrieval','ont:Computation':'k-means'}) 
		doc.usage(get_earnings, resource2014, startTime, None,
        			{prov.model.PROV_TYPE: 'ont:Retrieval','ont:Computation':'k-means'}) 
		doc.usage(get_earnings, resource2015, startTime, None,
        			{prov.model.PROV_TYPE: 'ont:Retrieval','ont:Computation':'k-means'}) 
		earnings = doc.entity('dat:shreya#earnings_kmeans',
        	{prov.model.PROV_LABEL:'K-Means of Employee Earnings By Year (2012-2015)',
        	prov.model.PROV_TYPE: 'ont:DataSet'})
		doc.wasAttributedTo(earnings, this_script)
		doc.wasGeneratedBy(earnings, get_earnings, endTime)
		doc.wasDerivedFrom(earnings, resource, get_earnings, get_earnings, get_earnings)


		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()

		return doc