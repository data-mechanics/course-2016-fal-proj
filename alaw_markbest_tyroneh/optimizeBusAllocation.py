import dml
import re
import numpy as np
from scipy.stats import norm
import matplotlib.pyplot as plt

class optimizeBusAllocation():
	contributor = 'alaw_markbest_tyroneh'
	reads = ['alaw_markbest_tyroneh.BusRoutes','alaw_markbest_tyroneh.AvgRouteVelocity']
	writes = []

	@staticmethod
	def area(m1,m2,std1,std2):
		'''returns area between two identical distributions N(m,std)'''

		intersect = (m1+m2)/2
		area = norm.cdf(intersect,m2,std2) + (1.-norm.cdf(intersect,m1,std1))
		return area

	@staticmethod
	def collectData():
		''''''

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
					route_times[route_id] = {'Avg Completion Time': distance/route_avgV, 'Completion Time Stdev': abs((distance/route_avgV) - distance/(route_avgV + route_stdV)), 'Optimum Allocation':0, 'Optimum Average Wait Time':0, 'Data count': len(v['value']['Velocities']), 'Stops count': len(r['properties']['route_stops'])}
		
		repo.logout()

		return route_times

	def runOptimization(route_times):
		''''''
		for route in route_times:
			avg = route_times[route]['Avg Completion Time']
			std = route_times[route]['Completion Time Stdev']
			n = route_times[route]['Stops count'] 

			#first score: 100% of original efficiency baseline (1 bus serving n stops)
			k_vals = []
			k_vals.append(avg * n)
			
			latency_vals = [avg * n]
			inefficiency_vals = [0]

			for k in range(1,100):
				#allocation of buses = k + 1
				latency = (avg/(k+1)) * n #latency * number of stops
				inefficiency = optimizeBusAllocation.area(0,(avg/k),std,std)*(k+1) #inefficiency * number of buses 
				score =  latency + inefficiency
				k_vals.append(score)
				latency_vals.append(latency)
				inefficiency_vals.append(inefficiency)


			plt.plot([x for x in range(1,101)], k_vals, color='green')
			plt.plot([x for x in range(1,101)], latency_vals, color='red')
			plt.plot([x for x in range(1,101)], inefficiency_vals, color='blue')
			route_times[route]['Optimum Allocation'] = k_vals.index(min(k_vals)) + 1
			route_times[route]['Optimum Average Wait Time'] = avg/(k_vals.index(min(k_vals))+1)

			
			print('-----------------')
			print(route)
			print(route_times[route])

			#break

		plt.show()
		return route_times


# 	def run():
# 		'''Outputs matplotlib scatterplot'''
# 		return optimizeBusAllocation.execute()

if __name__ == '__main__':
	result = optimizeBusAllocation.runOptimization(optimizeBusAllocation.collectData())
# ## eof