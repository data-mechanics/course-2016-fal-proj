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

class vector:
    def __init__(self, x, y, xFirst=True):
        if xFirst:
            self.x = x
            self.y = y
        else:
            self.x = y
            self.y = x

    @staticmethod
    def dist(u, v):
        return (v.x-u.x)**2 + (v.y-u.y)**2

    def __add__(self, other):
        return vector(self.x + other.x, self.y + other.y)

    def __sub__(self, other):
        return vector(self.x - other.x, self.y - other.y)

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y

    def __str__(self):
        return "v(" + str(self.x) + ", " + str(self.y) + ")"

    def __repr__(self):
        return self.__str__()

    def tup(self):
        return (self.x, self.y)

    def scale(self, c):
        ''' Returns a vector scaled by constant c '''
        return vector(c*self.x, c*self.y)

    def dot(self, v):
        return self.x * v.x + self.y * v.y

    def mag(self):
        return self.x*self.x + self.y*self.y

    def project(self, v):
        '''projects this vector onto vector v'''

        c = self.dot(v) / v.mag()
        return v.scale(c)

    def normal(self, v):
        p = self.project(v)

        norm = p - self

        in_xbounds = (v.x >= 0 and 0 <= p.x <= v.x) or (v.x <= 0 and v.x <= p.x <= 0)
        in_ybounds = (v.y >= 0 and 0 <= p.y <= v.y) or (v.y <= 0 and v.y <= p.y <= 0)

        if not (in_xbounds and in_ybounds):
            # find the closest endpoint of the line to the original point
            cl = min(vector(0, 0), v, key=lambda x: vector.dist(x, self))
            norm = cl - self
            #cl_x, cl_y = min((0, 0), v, key=lambda x: dist(x, u))
            #norm = (cl_x - ux, cl_y - uy)

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
        ''' Maps points on a multiline to a straight line based on their total distance from the first point'''
        qs = [0]
        q = 0
        for j in range(1, len(ps)):  
            q += vector.dist(ps[j-1], ps[j])
            qs.append(q)

        return qs

    def unmap1D(qs, points):
        ps = []
        for q in qs:
            accum = 0
            p1 = points[0]
            
            for i in range(1, len(points)):
                p2 = points[i]
                if vector.dist(p1, p2) == 0:
                    continue

                if q >= accum:
                    line = p2 - p1
                    diff = q - accum
                    p = p1 + line.scale(diff / line.mag())
                    ps.append(p)
                    break
                else:
                    accum += vector.dist(p1, p2)
            
                # only accept points that are far enough apart
                p1 = points[i]
                
        return ps
        
    def findStops(numStops, points):
        mapped_points = map1D(points)
        kmeans = KMeans(n_clusters=numStops, random_state=0)
        mapped_stops = kmeans.fit(mapped_points)
        return unmap1D(mapped_stops)

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
        # Collect point data and store in r tree
        routes_dict, stops_dict, points_dict = rTreeKmeans.collectData()
        tree, tree_keys = rTreeKmeans.rTreeify(points_dict)

        # find close points and project onto line
        route = routes_dict[str(route_num)]
        result_set = rTreeKmeans.routeTreeIntersection(routes_dict['29100'], tree, tree_keys, points_dict, r)
        proj = rTreeKmeans.projectPoints(route, points_dict, result_set, tree_keys)

        # run k-means to find stops
        num_stops = 10
        stops = findStops(10, proj)
        return routes_dict, stops_dict, points_dict, tree, tree_keys, result_set, proj, stops

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        pass
