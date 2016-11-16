import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
import ast
import math



def calculate(x0,y0,x1,y1):
	x_d_sq=pow(abs(x0)-abs(x1),2)
	y_d_sq=pow(abs(y0)-abs(y1),2)
	dist=math.sqrt(x_d_sq+y_d_sq)
	return dist

class distances(dml.Algorithm):
	contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
	reads = ['aliyevaa_bsowens_dwangus_jgtsui.community_indicators']
	writes = ['aliyevaa_bsowens_dwangus_jgtsui.distances']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(distances.contributor, distances.contributor)		
		repo.dropPermanent("distances")
		repo.createPermanent("distances")
		lines = [line.rstrip('\n') for line in open('centers.txt')]
		coordinates=[]
		point=[]
		for line in lines:
			if line!='':	
				point=[float(x) for x in line.split() ]	
				coordinates.append(point)
				point=[]
		
		out=open('out.txt', 'w')
		score=0
		count=0
		elem_n=0
		prep=[]
		for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.community_indicators.find():
			elem_n+=1
			for center in coordinates:	
				d=calculate(elem['location']['coordinates'][0], elem['location']['coordinates'][1], center[0],center[1])
				score=d*elem['community_score']+score
				entry={}
				entry.update({"cell_community_value": score})
				entry.update({"cell_center_latitude": elem['location']['coordinates'][0]})
				entry.update({"cell_center_longitude": elem['location']['coordinates'][1]})
				prep.append(entry)
			score=0

		str_prep=', '.join(json.dumps(d) for d in prep)
		l_prep='['+str_prep+']'
		r=json.loads(l_prep)
		repo['aliyevaa_bsowens_dwangus_jgtsui.distances'].insert_many(r)
		#json.dump(entry, out)					
		repo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client =  dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(distances.contributor,distances.contributor)
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
		doc.add_namespace('dat', 'http://datamechanics.io/data/') 
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/') 
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#distances',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource =doc.entity('bdp:city=Boston', {'prov:label':'calculating community values', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)	
		doc.wasAssociatedWith(get_liquor_data, this_script)

		doc.usage(get_liquor_data , resource, startTime, None)	
		found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#distances', {prov.model.PROV_LABEL:'calculating community values', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_liquor_data, endTime)
		doc.wasDerivedFrom(found, resource, get_liquor_data, get_liquor_data, get_liquor_data)
		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()
		return doc
distances.execute()
doc = distances.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
