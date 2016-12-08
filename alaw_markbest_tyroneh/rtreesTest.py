import dml
import prov.model
import datetime
import uuid
import json
import math
from vector import vector
import matplotlib.pyplot as plt
import numpy as np
from rtree import index
from collections import OrderedDict
from sklearn.cluster import KMeans

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
            #coordinates of endpoints
            yi, xi = multipoint[j-1]
            yj, xj = multipoint[j]

            intersecting_points = set()
            x_min = min(xi, xj) - r
            x_max = max(xi, xj) + r
            y_min = min(yi, yj) - r
            y_max = max(yi, yj) + r
            
            for i in list(tree.intersection((x_min,y_min,x_max,y_max))):
                intersecting_points.add(i)

            #for each point in the intersecting set, determine if it's within distance r of the line:
            
            for i in intersecting_points:
                key = tree_keys[str(i)]
                coor = points_dict[key]['geometry']['coordinates']

                if rTreeKmeans.coordinateDistance((xi, yi), (xj, yj), (coor[1], coor[0])) <= r:
                    result_set.add(i)

        return result_set


    def projectPoints(route, points_dict, close_points, tree_keys):
        '''project the intersecting points onto the route'''
        #for each pair of points, find points within the box of endpoints extended by r
        multipoint = route['geometry']['coordinates']

        # dictionary to store normal vectors from point to multiline
        normals = {}

        for j in range(1,len(multipoint)):
            end_i = vector(*multipoint[j-1], False)
            end_j = vector(*multipoint[j], False)

            line = end_j - end_i
            #line = (xj - xi, yj - yi)

            for i in close_points:
                key = tree_keys[str(i)]
                p = vector(*points_dict[key]['geometry']['coordinates'], False)

                vec = p - end_i

                #vec = (px - xi, py - yi)
                norm = (j, vec.normal(line))
                #norm = (j, normal(vec, line))

                if i not in normals:
                    normals[i] = norm
                else:
                    normals[i] = min(normals[i], norm, key=lambda x: x[1].mag())

        projections = OrderedDict()
        for i in range(1, len(multipoint)): projections[i] = [] # Initialize sort order of dict

        for i, norm in normals.items():
                key = tree_keys[str(i)]
                p = vector(*points_dict[key]['geometry']['coordinates'], False)

                line_num, n = norm

                projections[line_num].append((n+p))

        #sort the projection dictionary
        for j, points in projections.items():
            e = vector(*multipoint[j-1], False) # sort by distance from the 'starting' endpoint
            points.sort(key = lambda p: vector.dist(p, e))

        #return projections
        return [p for line in projections.values() for p in line]

    def map1D(ps):
        ''' Maps points on a multiline to a straight line based
            on an aggregated sum of the line distances
            Also returns a function that will unmap the 1D points '''
        qs = [0]
        q = 0
        for j in range(1, len(ps)):  
            q += vector.dist(ps[j-1], ps[j], squared=False)
            qs.append(q)

        def unmap1D(qs):
            return rTreeKmeans._unmap1D(qs, ps)

        return qs, unmap1D

    def _unmap1D(qs, points):
        ps = []
        for q in qs:
            accum = 0
            p1 = points[0]

            for i in range(1, len(points)):
                p2 = points[i]
                line = p2 - p1
                line_mag = line.mag(squared=False)
                if line_mag == 0:
                    continue

                accum += line_mag

                if accum >= q:
                    diff = accum - q
                    p = p1 + line.scale(diff / line_mag)
                    ps.append(p)
                    break

                # only accept points that are far enough apart
                p1 = p2

        return ps

    def findStops(numStops, points2D):
        ''' maps points to a 1D line and performs a kmeans, then unmaps the stops'''
        points1D, unmap1D = rTreeKmeans.map1D(points2D)

        # reshape points
        X = np.array(points1D).reshape(-1,1)

        kmeans = KMeans(n_clusters=numStops, random_state=0)
        mapped_stops = kmeans.fit(X).cluster_centers_
        stops = [stop.tup() for stop in unmap1D(mapped_stops)]
        return stops

    def visualize(route,points_dict,intersecting_points,tree_keys,using_idx=True):
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
            if using_idx:
                key = tree_keys[str(p)]
                intersect_x.append(points_dict[key]['geometry']['coordinates'][1])
                intersect_y.append(points_dict[key]['geometry']['coordinates'][0])
            else:
                intersect_x.append(p[0])
                intersect_y.append(p[1])

        plt.figure(figsize=(10,10))
        plt.scatter(point_x, point_y, color='blue')
        plt.scatter(intersect_x, intersect_y, color='red')
        plt.scatter(route_x, route_y, color='green')
        
        #plt.ylim(42.23,42.41)
        #plt.xlim(-71.18,-70.993)
        plt.show()

    def visualize_stops(route_dict, stops_dict, new_stops):
        '''check to see if a route on the map has indeed drawn the correct points in the intersecting set versus the rest of the points'''

        route_y, route_x = zip(*route_dict['geometry']['coordinates'])
        
        stop_ids_for_route = route_dict['properties']['route_stops']    
        old_stops = [stops_dict[str(stop_id)]['geometry']['coordinates'] for stop_id in stop_ids_for_route]
        
        old_stops_y, old_stops_x = zip(*old_stops)
        new_stops_x, new_stops_y = zip(*new_stops)
        
        plt.figure(figsize=(10,10))

        plt.plot(route_x, route_y, color='#e36c09', linewidth=1)
        plt.scatter(old_stops_x, old_stops_y, color='#4e74ba')
        plt.scatter(new_stops_x, new_stops_y, color='cyan')

        #plt.ylim(42.23,42.41)
        #plt.xlim(-71.18,-70.993)
        plt.show()

    def visualize_stops1(route_dict, stops_dict, new_stops, intersecting_points, tree_keys, points_dict):
            '''Create visualizations for each route for bus allocation, total average wait time, and total bus inefficiency'''

            fig = plt.figure(figsize=(14,10))
            fig.patch.set_facecolor('white')
            ax = plt.gca()

            ax.set_axis_bgcolor('white')
            ax.set_xticklabels([])
            ax.set_yticklabels([])
            #plt.grid(True)

            plt.title('', fontsize=48, y=1.03, fontweight='medium', color='#4e74ba')
            #plt.xlabel('Latitude',  fontsize=24, fontweight='medium', color='#e36c09')
            #plt.ylabel('Longitude', fontsize=24, fontweight='medium', color='#e36c09')

            os = mpatches.Patch(color='#96bbff', label='Original Stop locations')
            ns = mpatches.Patch(color='#0042ba', label='Kmeans derived locations')

            plt.legend(handles=[os, ns], fancybox=True, loc='best', fontsize=20)

            point_y, point_x = zip(*[points_dict[p]['geometry']['coordinates'] for p in points_dict])
                
            route_y, route_x = zip(*route_dict['geometry']['coordinates'])
        
            stop_ids_for_route = route_dict['properties']['route_stops']    
            old_stops = [stops_dict[str(stop_id)]['geometry']['coordinates'] for stop_id in stop_ids_for_route]
            old_stops_y, old_stops_x = zip(*old_stops)

            new_stops_x, new_stops_y = zip(*new_stops)
            
            inters = [tree_keys[str(i)] for i in intersecting_points]
            intersecting_points_y, intersecting_points_x = zip(*[points_dict[key]['geometry']['coordinates'] for key in inters])
            
            plt.plot(point_x, point_y, color='#96bbff', marker='.', linestyle='None', markersize=3, zorder=1)
            plt.plot(intersecting_points_x, intersecting_points_y, color='#0042ba', marker='.', linestyle='None', markersize=5, zorder=2)
            plt.plot(route_x, route_y, color='#999999', linestyle='-', linewidth=12, zorder=3)
            plt.scatter(old_stops_x, old_stops_y, color= '#ffc091', s=36.0, marker='D', zorder=4)
            plt.scatter(new_stops_x, new_stops_y, color='#ff6e00', s=36.0, marker='D', zorder=5)

            # margin
            m = 0.01
            xmin = min(min(route_x), min(new_stops_x), min(old_stops_x), min(intersecting_points_x)) - m 
            xmax = max(max(route_x), max(new_stops_x), max(old_stops_x), max(intersecting_points_x)) + m
            ymin = min(min(route_y), min(new_stops_y), min(old_stops_y), min(intersecting_points_y)) - m
            ymax = max(max(route_y), max(new_stops_y), max(old_stops_y), max(intersecting_points_y)) + m
            
            plt.xlim(xmin, xmax)
            plt.ylim(ymin, ymax)

            #plt.savefig('optimalAllocation.png')

            plt.show()

    @staticmethod
    def execute(route_num, r=0.01):
        # Collect point data and store in r tree
        routes_dict, stops_dict, points_dict = rTreeKmeans.collectData()
        tree, tree_keys = rTreeKmeans.rTreeify(points_dict)

        # find close points and project onto line
        route = routes_dict[str(route_num)]
        result_set = rTreeKmeans.routeTreeIntersection(route, tree, tree_keys, points_dict, r)
        proj = rTreeKmeans.projectPoints(route, points_dict, result_set, tree_keys)

        # run k-means to find stops
        num_stops = len(route['properties']['route_stops'])
        stops = rTreeKmeans.findStops(num_stops, proj)
        return routes_dict, stops_dict, points_dict, tree, tree_keys, result_set, proj, stops

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass
