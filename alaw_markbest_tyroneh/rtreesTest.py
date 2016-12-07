import dml
import prov.model
import datetime
import uuid
import json
import math
import matplotlib.pyplot as plt
from rtree import index
from collections import OrderedDict
from sklearn.cluster import KMeans

def dist(p1, p2):
    p1x, p1y = p1
    p2x, p2y = p2
    return (p2x-p1x)**2 + (p2y-p1y)**2

def mag(v):
    vx, vy = v
    return vx*vx + vy*vy

def project(u, v):
    '''projects vector u onto vector v'''
    ux, uy = u
    vx, vy = v

    uv_dot = (ux * vx) + (uy * vy)
    v_mag_sqr = (vx * vx) + (vy * vy)
    c = uv_dot / v_mag_sqr

    return (c*vx, c*vy)

def normal(u, v):
    ux, uy = u
    vx, vy = v
    px, py = project(u, v)

    norm = (px - ux, py - uy)

    in_xbounds = (vx >= 0 and 0 <= px <= vx) or (vx <= 0 and vx <= px <= 0)
    in_ybounds = (vy >= 0 and 0 <= py <= vy) or (vy <= 0 and vy <= py <= 0)

    if not (in_xbounds and in_ybounds):
        # find the closest endpoint of the line to the original point
        cl_x, cl_y = min((0, 0), v, key=lambda x: dist(x, u))
        norm = (cl_x - ux, cl_y - uy)

    return norm



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
            yi, xi = multipoint[j-1]
            yj, xj = multipoint[j]
            line = (xj - xi, yj - yi)

            for i in close_points:
                key = tree_keys[str(i)]
                py, px = points_dict[key]['geometry']['coordinates']

                vec = (px - xi, py - yi)
                norm = (j, normal(vec, line))

                if i not in normals:
                    normals[i] = norm
                else:
                    normals[i] = min(normals[i], norm, key=lambda x: mag(x[1]))

        projections = OrderedDict()
        for i in range(1, len(multipoint)): projections[i] = [] # Initialize sort order of dict

        for i, norm in normals.items():
                key = tree_keys[str(i)]
                py, px = points_dict[key]['geometry']['coordinates']
                line_num = norm[0]
                nx, ny = norm[1]

                projections[line_num].append((nx+px, ny+py))

        #sort the projection dictionary
        for j, points in projections.items():
            ey, ex = multipoint[j-1] # sort by distance from the 'starting' endpoint
            points.sort(key = lambda p: dist(p, (ex, ey)))

        #return projections
        return [p for line in projections.values() for p in line]  

    def findStops(numStops, points):
        kmeans = KMeans(n_clusters=numStops, random_state=0)
        kmeans.fit(points)

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

    @staticmethod
    def execute(route_num, r=0.01):
        routes_dict, stops_dict, points_dict = rTreeKmeans.collectData()
        tree, tree_keys = rTreeKmeans.rTreeify(points_dict)
        route = routes_dict[str(route_num)]
        result_set = rTreeKmeans.routeTreeIntersection(routes_dict['29100'], tree, tree_keys, points_dict, r)
        proj = rTreeKmeans.projectPoints(route, points_dict, result_set, tree_keys)
        return routes_dict, stops_dict, points_dict, tree, tree_keys, result_set, proj

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass
