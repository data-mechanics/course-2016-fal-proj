import dml
import prov.model
import datetime
import uuid
import json
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
		properties_dict = {}
		for d in data:
			properties_dict[str(d['_id'])] = {'type':d['value']['type'],'geometry':d['value']['geometry'],'properties':d['value']['properties']}

		data = [i for i in repo['alaw_markbest_tyroneh.StationsGeoJSONs'].find()] #unflattened
		stations_dict = {}
		for d in data:
			stations_dict[str(d['_id'])] = {'type':d['value']['type'],'geometry':d['value']['geometry'],'properties':d['value']['properties']}

		repo.logout()

		return routes_dict,stops_dict,properties_dict,stations_dict

	def rTreeify(properties,stations):
		'''take GeoJSONs and construct rTree for intersection search'''

		tree = index.Index()
		tree_keys = []
		i = 0
		for p in properties:
			tree_keys.append(p)
			lat = properties[p]['geometry']['coordinates'][0]
			lon = properties[p]['geometry']['coordinates'][1]
			tree.insert(i,(lat,lon,lat,lon))
			i += 1

		for s in stations:
			tree_keys.append(s)
			lat = stations[s]['geometry']['coordinates'][0]
			lon = stations[s]['geometry']['coordinates'][1]
			tree.insert(i,(lat,lon,lat,lon))
			i += 1

		return tree, tree_keys

	@staticmethod
	def execute():
		pass

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass