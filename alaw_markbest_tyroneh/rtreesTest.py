import dml
import prov.model
import datetime
import uuid
import json
import math
import matplotlib.pyplot as plt
from rtree import index


class rTreeKmeans(dml.Algorithm):
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

	def coordinateDistance(xi, yi, xj, yj, x, y):
		'''calculate distance of point to line using formula'''

		dist = abs((yj - yi) * x - (xj - xi) * y + (xj * yi) - (xi * yj))
		return dist / (( ((yj - yi) ** 2) + ((xj - xi) ** 2) ) ** 0.5)

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

				if rTreeKmeans.coordinateDistance(xi, yi, xj, yj, coor[1], coor[0]) <= r:
					result_set.add(i)

		return result_set

	@staticmethod
	def execute():
		pass

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass