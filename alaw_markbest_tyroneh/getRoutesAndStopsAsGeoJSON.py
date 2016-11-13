import urllib.request
import json
import dml
import csv
import prov.model
import datetime
import uuid
import xmltodict
import zipfile
import shapefile
import io
import sys
import os
import pyproj
from dbfread import DBF
import re
import shutil

gisURLs = {"BusRoutesAndStops":'http://wsgw.mass.gov/data/gispub/shape/state/mbtabus.zip'}
gisKeys = {"BusRoutes":'MBTABUSROUTES_ARC', "BusStops":'MBTABUSSTOPS_PT'}
dbfKeys = {"RoutesToStops":'MBTABUSSTOPS_PT_EVENTS.dbf'}

# Get the data from the server.
response = urllib.request.urlopen(gisURLs['BusRoutesAndStops'])

# Unzip the file into the working directory.
zip_ref = zipfile.ZipFile(io.BytesIO(response.read()))
zip_ref.extractall("./")
zip_ref.close()

# Function to reverse the Lambert projection done on the route/stop data and convert to latitude/longitude.
reverseCoordinateProjection = pyproj.Proj(proj = 'lcc', datum = 'NAD83', lat_1 = 41.71666666666667, lat_2 = 42.68333333333333,lat_0 = 41.0, lon_0 = -71.5, x_0 = 200000.0, y_0 = 750000.0)

# Read the MBTA Bus Routes file into the Python shapefile library.
sfRoute = shapefile.Reader(gisKeys['BusRoutes'])

# List comprehension to pull out route coordinates and related data in GeoJSON format.
geoJSONRoutes = [
{
    "type":"Feature",
    "geometry":{
        "type":"Multipoint",
        "coordinates": [reverseCoordinateProjection(p[0], p[1], inverse = True) for p in x.shape.points]},
    "properties":{
        "route_name": x.record[8],
        "route_id": x.record[1],
        "route_variant": x.record[2],
        "direction":x.record[6],
        "ctps_id":x.record[9],
        "route_stops":[],
        "route_distance": 0
    }
}
for x in [i for i in sfRoute.shapeRecords()] ]

# Drop routes that aren't metropolitan routes (numeric route_id in range(1,121))
geoJSONRoutes = [i for i in geoJSONRoutes if(int(re.sub("[^0-9]", "", i['properties']['route_id'])) in range(1,122))]

# Drop routes in one direction (outbound only)
geoJSONRoutes = [i for i in geoJSONRoutes if(i['properties']['direction'] == 0)]

# Read the MBTA Bus Stops file into the Python shapefile library.
sfStops = shapefile.Reader(gisKeys['BusStops'])

# List comprehension to pull out stop coordinates and related data in GeoJSON format.
geoJSONStops = [
{
    "type":"Feature",
    "geometry":{
        "type":"Point",
        "coordinates": reverseCoordinateProjection(x.shape.points[0][0], x.shape.points[0][1], inverse = True) },
    "properties":{
        "stop_name": x.record[1],
        "stop_id": x.record[0],
        "town":x.record[2],
        "route_list":[]
    }
}
for x in [i for i in sfStops.shapeRecords()]]

# Read the MBTA Routes to Stops file into the Python dbf library.
sfRTS = DBF(dbfKeys['RoutesToStops'], load=True)
records = sfRTS.records

# keep track of relevant stops
usable_stops = set()

#associate routes and stops together & store of total length per routes
for stop in records:
    stop_id = stop['STOP_ID']
    ctps_id = stop['CTPS_ROU_1']
    # convert to km
    distance = stop['MEASURE'] * 1.60934

    routes = [route for route in geoJSONRoutes if route['properties']['ctps_id'] == ctps_id]
    stops = [stop for stop in geoJSONStops if stop['properties']['stop_id'] == stop_id]

    for r in routes:
        r['properties']['route_stops'].append(stop_id)
        r['properties']['route_distance'] = max([r['properties']['route_distance'],distance])

    for s in stops:
        s['properties']['route_list'].append(ctps_id)

    if(len(routes) > 0):
        usable_stops.add(stop_id)

# Drop routes not part of metropolitan routes

geoJSONStops = [i for i in geoJSONStops if i['properties']['stop_id'] in usable_stops]



