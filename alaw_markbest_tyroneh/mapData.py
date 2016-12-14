import dml
import prov.model
import datetime
import uuid
import json
import math
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from rtree import index

class mapData(dml.Algorithm):
	contributor = 'alaw_markbest_tyroneh'
	reads = ['alaw_markbest_tyroneh.BusRoutes','alaw_markbest_tyroneh.BusStops','alaw_markbest_tyroneh.PropertyGeoJSONs','alaw_markbest_tyroneh.StationsGeoJSONs']
	writes = []

	# brew install spatialindex
	# pip install rtree
	
	def collectData():
		'''retrieves BusRoutes, BusStops, PropertyGeoJSONs, StationsGeoJSONs datasets for optimization purposes'''

		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('alaw_markbest_tyroneh', 'alaw_markbest_tyroneh')

		data = [i for i in repo['alaw_markbest_tyroneh.BusRoutes'].find()]
		routes_dict = {}
		for d in data:
			routes_dict[str(d['properties']['ctps_id'])] = {'type':d['type'],'geometry':d['geometry'],'properties':d['properties']}

		data = [i for i in repo['alaw_markbest_tyroneh.BusStops'].find()]
		stops_dict = {}
		for d in data:
			stops_dict[str(d['properties']['stop_id'])] = {'type':d['type'],'geometry':d['geometry'],'properties':d['properties']}

		data = [i for i in repo['alaw_markbest_tyroneh.PropertyGeoJSONs'].find()] #unflattened
		points_dict = {}
		for d in data:
			points_dict[str(d['_id'])] = {'type':d['value']['type'],'geometry':d['value']['geometry'],'properties':d['value']['properties']}

		data = [i for i in repo['alaw_markbest_tyroneh.StationsGeoJSONs'].find()] #unflattened
		for d in data:
			points_dict[str(d['_id'])] = {'type':d['value']['type'],'geometry':d['value']['geometry'],'properties':d['value']['properties']}

		repo.logout()

		return routes_dict,stops_dict,points_dict

	def rTreeify(points_dict):
		'''take GeoJSONs and construct rTree for intersection search'''

		tree = index.Index()
		tree_keys = {}
		i = 0
		for p in points_dict:
			tree_keys[str(i)] = p
			y = points_dict[p]['geometry']['coordinates'][0]
			x = points_dict[p]['geometry']['coordinates'][1]
			tree.insert(i,(x,y,x,y))
			i += 1

		return tree, tree_keys

	def coordinateDistance(v,w,p):
		'''find distance of point to line segment'''
		seglen_squared = (v[0] - w[0])**2 + (v[1] - w[1])**2
		#t = max(0, min(1, dot(p - v, w - v) / l2))
		t =  max(0, min(1, ((p[0]-v[0])*(w[0]-v[0]) + (p[1]-v[1])*(w[1]-v[1]))/seglen_squared ))
		#projection = v + t * (w - v)
		projection = (t*(w[0] - v[0])+v[0],t*(w[1] - v[1])+v[1])
		return ((p[0] - projection[0])**2 + (p[1] - projection[1])**2 )**0.5

	def routeTreeIntersection(route,tree,tree_keys,points_dict, r):
		'''Using route from dictionary and the points tree, return list of keys that are within distance r of the route'''

		multipoint = route['geometry']['coordinates']
		
		result_set = set()

		#for each pair of points, find points within the box of endpoints extended by r
		for j in range(1,len(multipoint)):
			i = j-1

			#coordinates of endpoints
			yi, xi = multipoint[i]
			yj, xj = multipoint[j]

			intersecting_points = set()

			#if endpoint i is to the left of endpoint j
			if(xi < xj):
				# if endpoint i is above endpoint j
				if(yi > yj):
					x_min = xi - r
					x_max = xj + r 
					y_min = yj - r
					y_max = yi + r
					for i in list(tree.intersection((x_min,y_min,x_max,y_max))):
						intersecting_points.add(i)

				# if endpoint i is below endpoint j
				else:
					x_min = xi - r
					x_max = xj + r 
					y_min = yi - r
					y_max = yj + r
					for i in list(tree.intersection((x_min,y_min,x_max,y_max))):
						intersecting_points.add(i)

			#if endpoint i is to the right of endpoint j
			else:
				# if endpoint i is above endpoint j
				if(yi > yj):
					x_min = xj - r
					x_max = xi + r
					y_min = yj - r
					y_max = yi + r
					for i in list(tree.intersection((x_min,y_min,x_max,y_max))):
						intersecting_points.add(i)
						
				# if endpoint i is below endpoint j
				else:
					x_min = xj - r
					x_max = xi + r
					y_min = yi - r
					y_max = yj + r
					for i in list(tree.intersection((x_min,y_min,x_max,y_max))):
						intersecting_points.add(i)
		
			#for each point in the intersecting set, determine if it's within distance r of the line:
			for i in intersecting_points:
				key = tree_keys[str(i)]
				coor = points_dict[key]['geometry']['coordinates']

				if mapData.coordinateDistance((xi, yi),(xj, yj), (coor[1], coor[0])) <= r:
					result_set.add(i)

		return result_set

	def visualize(routes_dict,route,points_dict,intersecting_points,tree_keys):
		'''check to see if a route on the map has indeed drawn the correct points in the intersecting set versus the rest of the points'''

		route_x = [c[1] for c in route['geometry']['coordinates']]
		route_y = [c[0] for c in route['geometry']['coordinates']]

		point_x = []
		point_y = []
		for p in points_dict:
			point_x.append(points_dict[p]['geometry']['coordinates'][1])
			point_y.append(points_dict[p]['geometry']['coordinates'][0])

		intersect_x = []
		intersect_y = []
		for p in intersecting_points:
			key = tree_keys[str(p)]
			intersect_x.append(points_dict[key]['geometry']['coordinates'][1])
			intersect_y.append(points_dict[key]['geometry']['coordinates'][0])

		routes_x = []
		routes_y = []

		for r in routes_dict:
			routes_x.append([c[1] for c in routes_dict[r]['geometry']['coordinates']])
			routes_y.append([c[0] for c in routes_dict[r]['geometry']['coordinates']])

		fig = plt.figure(figsize=(18,10))
		fig.patch.set_facecolor('white')
		ax = plt.gca()

		ax.set_xticklabels([])
		ax.set_yticklabels([])

		plt.scatter(point_x, point_y, color='#96bbff',zorder=1, s=0.3)

		plt.scatter(intersect_x, intersect_y, color='#0042ba',zorder=2, s=0.6)

		for i in range(len(routes_x)):
			plt.plot(routes_x[i],routes_y[i], color='#ffc091', zorder=3, linewidth=0.8, ls='dashed')

		plt.plot(route_x, route_y, color='#ff6e00',zorder=4, linewidth=4)

		ri = mpatches.Patch(color='#96bbff', label='ROI')
		ov = mpatches.Patch(color='#0042ba', label='Coverage')
		br = mpatches.Patch(color='#ffc091', label='Bus Routes')
		kv = mpatches.Patch(color='#ff6e00', label='Route 39')

		plt.legend(handles=[ri,ov,br,kv], fancybox=True, loc=4, fontsize=20)

		plt.ylim(42.23,42.41)
		plt.xlim(-71.18,-70.993)

		plt.savefig('mapSampleCoverage.png')

		plt.show()

	@staticmethod
	def execute():
		pass

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass