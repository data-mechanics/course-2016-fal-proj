import dml
import prov.model
import datetime
import uuid
import re
import numpy as np
from scipy.stats import norm
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import json

class optimizeBusAllocation(dml.Algorithm):
	contributor = 'alaw_markbest_tyroneh'
	reads = ['alaw_markbest_tyroneh.BusRoutes','alaw_markbest_tyroneh.AvgRouteVelocity']
	writes = ['alaw_markbest_tyroneh.OptimumAllocation']

	def area(m1,m2,std1,std2):
		'''returns area between two identical distributions N(m,std)'''

		intersect = (m1+m2)/2
		area = norm.cdf(intersect,m2,std2) + (1.-norm.cdf(intersect,m1,std1))
		return area

	def collectData():
		'''retrieves AvgRouteVelocity and BusRoutes datasets for optimization purposes'''

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('alaw_markbest_tyroneh', 'alaw_markbest_tyroneh')
		
		#pull AvgRouteVelocity data
		Vdataset = repo['alaw_markbest_tyroneh.AvgRouteVelocity'].find()

		#collect and calculate average route completion time and deviation
		route_times = {}

		for v in Vdataset:
			route_id = v['_id']
			route_avgV = v['value']['Average Velocity']
			route_stdV = np.std(v['value']['Velocities'])
			for r in repo['alaw_markbest_tyroneh.BusRoutes'].find():
				if(re.sub("[^0-9]", "",r['properties']['route_id']) == route_id):
					distance = r['properties']['route_distance']
					route_times[route_id] = {'Avg Time': distance/route_avgV, 'Time Stdev': abs((distance/route_avgV) - distance/(route_avgV + route_stdV)), 'Data count': len(v['value']['Velocities']), 'Stops count': len(r['properties']['route_stops'])}
		
		repo.logout()

		return route_times

	def runOptimization(route_times, trial=False):
		'''Using dictionary from collectData, compute optimum allocation of buses'''

		#dictionary for visualization purposes
		results = {}

		#output dataset for mongoDB
		output = []

		for route in route_times:
			avg = route_times[route]['Avg Time']
			std = route_times[route]['Time Stdev']
			n = route_times[route]['Stops count'] 

			#first score in k_vals: 100% of original latency baseline (1 bus serving n stops)
			k_vals = [avg * n]
			latency_vals = [avg * n]
			inefficiency_vals = [0]

			for k in range(1,100):
				
				#allocation of buses = k + 1
				latency = (avg/(k+1)) * n #latency * number of stops
				inefficiency = optimizeBusAllocation.area(0,(avg/k),std,std)*(k+1) #inefficiency * number of buses 
				
				#k score = decrease in latency vs increase in inefficiency
				score =  latency + inefficiency

				k_vals.append(score)
				latency_vals.append(latency)
				inefficiency_vals.append(inefficiency)

			#store route's optimum in output
			output.append({'Route': route, 'Avg Time': avg, 'Time Stdev': std, 'Stops Count': n, 'Allocation Scores': k_vals, 'Optimum Allocation': k_vals.index(min(k_vals)) + 1, 'Optimum Avg Time': avg/(k_vals.index(min(k_vals))+1)})
			results[route] = [route,k_vals,latency_vals,inefficiency_vals]

			#trial mode: only calculate for 1 route
			if(trial):
				break

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('alaw_markbest_tyroneh', 'alaw_markbest_tyroneh')
		
		# Set up the database connection for routes
		repo.dropPermanent('OptimumAllocation')
		repo.createPermanent('OptimumAllocation')
		repo['alaw_markbest_tyroneh.OptimumAllocation'].insert_many(output)

		repo.logout()

		return results

	def visualize(results):
		'''Create visualizations for each route for bus allocation, total average wait time, and total bus inefficiency'''

		fig = plt.figure(figsize=(14,10))
		fig.patch.set_facecolor('white')
		ax = plt.gca()
		
		optimal_x = []
		optimal_y = []
		route_opts = []

		#add plots for each route (blue: allocation, green: wait time, red: inefficiency)
		for r in results:
			route = results[r][0]
			k_vals = results[r][1]
			#plt.plot(range(1,101), latency_vals, color='green')
			#plt.plot(range(1,101), inefficiency_vals, color='red')
			plt.plot(range(1,101), k_vals, color='#4e74ba', label = route, zorder=1)
			optimal_i = k_vals.index(min(k_vals)) + 1
			optimal_x.append(optimal_i)
			optimal_y.append(k_vals[optimal_i])
			route_opts.append(route)

		plt.scatter(optimal_x,optimal_y, color='#e36c09',zorder=2)

		ax.set_axis_bgcolor('white')
		plt.grid(True)

		plt.xlabel('Buses Allocation (K)',  fontsize=24, fontweight='medium', color='#e36c09')
		plt.ylabel('Allocation Score (S)', fontsize=24, fontweight='medium', color='#e36c09')

		#lv = mpatches.Patch(color='green', label='Latency')
		#iv = mpatches.Patch(color='red', label='Inefficiency')
		kv = mpatches.Patch(color='#4e74ba', label='Allocation Score Curve')
		ov = mpatches.Patch(color='#e36c09', label='Optimal Score')

		plt.legend(handles=[kv,ov], fancybox=True, loc=7, fontsize=20)


		plt.xlim(1,25)
		plt.ylim(0,120)

		plt.savefig('optimalAllocation.png')

		plt.show()

	def outputScores():
		'''collect allocation scores for each route'''

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('alaw_markbest_tyroneh', 'alaw_markbest_tyroneh')

		data = repo['alaw_markbest_tyroneh.OptimumAllocation'].find()

		output = {}

		for d in data:
			route_name = d['Route']
			scores = d['Allocation Scores']
			k = d['Optimum Allocation']

			output[route_name] = {"scores": scores, "optimal": k}

		repo.logout()

		json.dump(output,open('routeScores.json','w'))
		
		return output

	@staticmethod
	def execute(trial = False, visual = False):	
		startTime = datetime.datetime.now()
		results = optimizeBusAllocation.collectData()
		output = optimizeBusAllocation.runOptimization(results, trial=trial)
		if(visual):
			optimizeBusAllocation.visualize(output)
		optimizeBusAllocation.outputScores()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		'''
		Create the provenance document describing everything happening
		in this script. Each run of the script will generate a new
		document describing that invocation event.
		'''

		 # Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('alaw_markbest_tyroneh', 'alaw_markbest_tyroneh')

		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

		this_script = doc.agent('alg:alaw_markbest_tyroneh#optimizeBusAllocation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

		BusRoutes = doc.entity('dat:alaw_markbest_tyroneh#BusRoutes', {prov.model.PROV_LABEL:'Bus Routes', prov.model.PROV_TYPE:'ont:DataSet'})        
		get_BusRoutes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Algorithm to produce Optimum Allocation'})        
		doc.wasAssociatedWith(get_BusRoutes, this_script)
		doc.used(get_BusRoutes, BusRoutes, startTime)
		doc.wasAttributedTo(BusRoutes, this_script)
		doc.wasGeneratedBy(BusRoutes, get_BusRoutes, endTime) 

		AvgRouteVelocity = doc.entity('dat:alaw_markbest_tyroneh#AvgRouteVelocity', {prov.model.PROV_LABEL:'Avg Route Velocity', prov.model.PROV_TYPE:'ont:DataSet'})        
		get_AvgRouteVelocity = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Algorithm to produce Optimum Allocation'})        
		doc.wasAssociatedWith(get_AvgRouteVelocity, this_script)
		doc.used(get_AvgRouteVelocity, AvgRouteVelocity, startTime)
		doc.wasAttributedTo(AvgRouteVelocity, this_script)
		doc.wasGeneratedBy(AvgRouteVelocity, get_AvgRouteVelocity, endTime) 

		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()

		return doc

	def run(trial = False, visual = False):
		'''
		Scrap datasets and write provenance files
		set v = True for visualizations
		'''
		times = optimizeBusAllocation.execute(trial = trial, visual = visual)
		optimizeBusAllocation.provenance(startTime = times['start'], endTime = times['end'])

# if __name__ == '__main__':
# 	optimizeBusAllocation.run(v=True)
# ## eof
