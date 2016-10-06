import xlrd
from collections import OrderedDict
from urllib import request
import urllib
import json
import dml
import prov.model
import datetime
import uuid
import csv


from pymongo import MongoClient


class bicycle_collisions(dml.Algorithm):
	contributor = 'aliyevaa_jgtsui'
	reads = []
	writes = ['aliyevaa_jgtsui.bicycle_collisions']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = MongoClient()
		mrepo = client.repo
		mrepo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')
		
		mrepo.dropPermanent("bicycle_collisions")
		mrepo.createPermanent("bicycle_collisions")
		'''
		url = "http://datamechanics.io/data/anuragp1_jl101995/weather.csv"
		urllib.request.urlretrieve(url, 'weather.csv')
		weather_df = pd.DataFrame.from_csv('weather.csv')
		repo['anuragp1_jl101995.weather'].insert_many(weather_df.to_dict('records'))
		os.remove('weather.csv')
		'''
		url ='http://datamechanics.io/data/aliyevaa_jgtsui/fbcd.xlsx'
		urllib.request.urlretrieve(url, "fbcd.xlsx")
	
		wb = xlrd.open_workbook('fbcd.xlsx')
		sheet = wb.sheet_by_index(0)
		accidents_list=[]
		for rownum in range(1, sheet.nrows):
			#print(rownum)
			attr = OrderedDict()
			row_values = sheet.row_values(rownum)
			attr['id'] = row_values[0]
			attr['year'] = row_values[1]
			attr['date'] = row_values[2]
			attr['day_week'] = row_values[3]
			attr['address'] = row_values[11]
			attr['nhood']=row_values[18]
			attr['lighting']=row_values[25]
			accidents_list.append(attr)
		
		s=json.dumps(accidents_list, sort_keys=True, indent=2)
		mrepo['aliyevaa_jgtsui.bicycle_collisions'].insert_many(accidents_list)
		mrepo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client = MongoClient()
		repo = client.repo
		repo.authenticate('aliyevaa_jgtsui', 'aliyevaa_jgtsui')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/aliyevaa_jgtsui')
		doc.add_namespace('dat', 'http://datamechanics.io/data/aliyevaa_jgtsui') 
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/') 
		doc.add_namespace('bdp', 'https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/24713')

		this_script = doc.agent('alg:aliyevaa_jgtsui#bicycle_collisions',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource =doc.entity('bdp:24713', {'prov:label':'Bicycle Collisions', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)	
		doc.wasAssociatedWith(get_liquor_data, this_script)

		doc.usage(get_liquor_data , resource, startTime, None)	
		found = doc.entity('dat:aliyevaa_jgtsui#bicycle_collisions', {prov.model.PROV_LABEL:'Bicycle Collisions', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_liquor_data, endTime)
		doc.wasDerivedFrom(found, resource, get_liquor_data, get_liquor_data, get_liquor_data)
		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()
		return doc


bicycle_collisions.execute()
doc = bicycle_collisions.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
