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
import pyproj
from dbfread import DBF
import re
import os
import shutil
import random

class getData(dml.Algorithm):
    contributor = 'alaw_markbest_tyroneh'
    reads = []
    writes = ['alaw_markbest_tyroneh.BostonProperty','alaw_markbest_tyroneh.CambridgeProperty','alaw_markbest_tyroneh.SomervilleProperty','alaw_markbest_tyroneh.BrooklineProperty', 'alaw_markbest_tyroneh.HubwayStations', 'alaw_markbest_tyroneh.TCStops','alaw_markbest_tyroneh.TimedBuses', 'alaw_markbest_tyroneh.CensusPopulation','alaw_markbest_tyroneh.BusRoutes','alaw_markbest_tyroneh.BusStops']

    @staticmethod
    def execute(trial = False):
        '''Retrieve datasets for mongoDB storage and later transformations'''
        
        startTime = datetime.datetime.now()
            
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alaw_markbest_tyroneh', 'alaw_markbest_tyroneh')
        
        #JSON urls with SoQL queries
        jsonURLs = {"BostonProperty": 'https://data.cityofboston.gov/resource/jsri-cpsq.json?$limit=11000000',
                    "CambridgeProperty": 'https://data.cambridgema.gov/resource/ufnx-m9uc.json?$limit=11000000',
                    "SomervilleProperty":'https://data.somervillema.gov/resource/dhs3-5kuu.json?$limit=11000000'}
        
        for key in jsonURLs:  
            url = jsonURLs[key]

            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)

            #Trial mode: run Algo R for 1000 samples
            if(trial == True):
                k = 1000
                sample = r[0:k]
                for i in range(2000,len(r)):
                    j = random.randint(0, i)
                    if(j <= k-1):
                        sample[j] = r[i]
                r = sample


            # Set up the database connection
            repo.dropPermanent(key)
            repo.createPermanent(key)
            repo['alaw_markbest_tyroneh.'+key].insert_many(r)
        
        #GeoJSON urls with queries, read list of features
        geojsonURLs = {"BrooklineProperty":"http://data.brooklinema.gov/datasets/a725742a993f425ea463c2c509d91ca3_5.geojson"}
        
        for key in geojsonURLs:  
            url = geojsonURLs[key]
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)['features']

            #Trial mode: run Algo R for 1000 samples
            if(trial == True):
                k = 1000
                sample = r[0:k]
                for i in range(2000,len(r)):
                    j = random.randint(0, i)
                    if(j <= k-1):
                        sample[j] = r[i]
                r = sample
            
            # Set up the database connection
            repo.dropPermanent(key)
            repo.createPermanent(key)
            repo['alaw_markbest_tyroneh.'+key].insert_many(r) 
                
        
        #CSV urls, converts them to python dictionaries 
        csvURLs = {"HubwayStations":'https://s3.amazonaws.com/hubway-data/2016_0429_Hubway_Stations.csv'}
        csvFields = {"HubwayStations":['Station','Station ID','Latitude','Longitude','Municipality','# of Docks']}        
        
        for key in csvURLs:
            url = csvURLs[key]
            fieldnames = csvFields[key]
            
            response = urllib.request.urlopen(url)
            csvfile = list(csv.DictReader(response.read().decode('utf-8').splitlines()[1:],fieldnames))

            #Trial mode: run Algo R for 1000 samples
            if(trial == True):
                k = 1000
                sample = csvfile[0:k]
                for i in range(2000,len(csvfile)):
                    j = random.randint(0, i)
                    if(j <= k-1):
                        sample[j] = csvfile[i]
                csvfile = sample
            
            # Set up the database connection
            repo.dropPermanent(key)
            repo.createPermanent(key)
            repo['alaw_markbest_tyroneh.'+key].insert_many(csvfile) 
        
        
        #MBTA API, gets xml files of every T and Commuter rail stop
        route_url = "http://realtime.mbta.com/developer/api/v2/routes?api_key=wX9NwuHnZU2ToO7GmGR9uw&format=json"

        response = urllib.request.urlopen(route_url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        routesA = [mode for mode in r["mode"] if mode["mode_name"] == "Subway" or mode["mode_name"] == "Commuter Rail"]

        routesB = [(mode["mode_name"], route["route_id"]) for mode in routesA for route in mode['route']]

        stop_base = "http://realtime.mbta.com/developer/api/v2/stopsbyroute?api_key=wX9NwuHnZU2ToO7GmGR9uw"
        stop_urls = {route:"{}&route={}&format=json".format(stop_base, route[1]) for route in routesB}
        responses = {route:urllib.request.urlopen(stop_urls[route]).read().decode("utf-8") for route in stop_urls}

        json_stops = []
        for route, response in responses.items():
            stops_by_route = {}

            mode, route_id = route
    
            stops_by_route['name'] = route_id 
            stops_by_route['mode'] = mode
            stops_by_route['path'] = json.loads(response)

            json_stops.append(stops_by_route)

        #json_stops = {oute':route, 'path':json.loads(responses[route]) for route in responses}
        #stops_dumps = {route:json.dumps(json_stops[route], sort_keys=True, indent=2) for route in json_stops}
        stops_dumps = json.dumps(json_stops, sort_keys=True, indent=2)

        result = []
        for route in json_stops:
            result.append({'route':route})

        #Trial mode: run Algo R for 1000 samples
        if(trial == True):
            k = 1000
            sample = result[0:k]
            for i in range(2000,len(result)):
                j = random.randint(0, i)
                if(j <= k-1):
                    sample[j] = result[i]
            result = sample

        # Set up the database connection
        repo.dropPermanent('TCStops')
        repo.createPermanent('TCStops')
        repo['alaw_markbest_tyroneh.TCStops'].insert_many(result) 

        #NextBus API: Retrieve scraped real time bus location data hosted on datamechanics.io website
        url_base = "http://datamechanics.io/data/alaw_markbest_tyroneh/busdata/mbtabuses-"
        bus_time = 1478798401
        last_bus_time = 1479073201
        all_buses = []
 
        while bus_time <= last_bus_time:
            url = url_base + str(bus_time)
            try:
                response = urllib.request.urlopen(url)
                response = response.read().decode("utf-8")
                r = json.loads(response)
                r['timestamp'] = bus_time # change timestamp to time of scraping
                all_buses.append(r)
            except urllib.error.HTTPError as e:
                pass
    
            bus_time += 300 # increment by 30 minutes

        #Trial mode: run Algo R for 1000 samples
        if(trial == True):
            k = 1000
            sample = all_buses[0:k]
            for i in range(2000,len(all_buses)):
                j = random.randint(0, i)
                if(j <= k-1):
                    sample[j] = all_buses[i]
            all_buses = sample
 
        repo.dropPermanent('TimedBuses')
        repo.createPermanent('TimedBuses')
        repo['alaw_markbest_tyroneh.TimedBuses'].insert_many(all_buses)  

    	#GIS data/shapefiles urls: converts them to python dictionaries
        CensusGisURLs = {"CensusPopulation":'http://wsgw.mass.gov/data/gispub/shape/census2010/CENSUS2010TOWNS_SHP.zip'}
        CensusGisTowns = ['BOSTON','BROOKLINE','CAMBRIDGE','SOMERVILLE']

        for key in CensusGisURLs:
            url = CensusGisURLs[key]

            # Get the data from the server.
            response = urllib.request.urlopen(url)

            # Unzip the file into the working directory.
            zip_ref = zipfile.ZipFile(io.BytesIO(response.read()))
            zip_ref.extractall("./")
            zip_ref.close()

            # Read the file into the Python shapefile library.
            sf = shapefile.Reader("CENSUS2010TOWNS_POLY")

            # Pull out the specific records for the four areas of interest, listed in CensusGisTowns above.
            # NOTE: The attribute at index 1 of any record is the uppercase town name.
            # stores dictionary with town name as key and 2010 population as data

            boston_area = [{'area':x[1],'population':x[9]} for x in sf.iterRecords() if x[1] in CensusGisTowns]

            #Trial mode: run Algo R for 1000 samples
            if(trial == True):
                k = 1000
                sample = boston_area[0:k]
                for i in range(2000,len(boston_area)):
                    j = random.randint(0, i)
                    if(j <= k-1):
                        sample[j] = boston_area[i]
                boston_area = sample

            # Set up the database connection
            repo.dropPermanent(key)
            repo.createPermanent(key)
            repo['alaw_markbest_tyroneh.'+key].insert_many(boston_area)


        # remove files after unzipping
        for path in ['CENSUS2010TOWNS_ARC.dbf','CENSUS2010TOWNS_ARC.prj','CENSUS2010TOWNS_ARC.sbn','CENSUS2010TOWNS_ARC.sbx','CENSUS2010TOWNS_ARC.shp','CENSUS2010TOWNS_ARC.shp.xml',
            'CENSUS2010TOWNS_ARC.shx','CENSUS2010TOWNS_POLY.cpg','CENSUS2010TOWNS_POLY.dbf','CENSUS2010TOWNS_POLY.prj','CENSUS2010TOWNS_POLY.sbn','CENSUS2010TOWNS_POLY.sbx',
            'CENSUS2010TOWNS_POLY.shp','CENSUS2010TOWNS_POLY.shp.xml','CENSUS2010TOWNS_POLY.shx']:
            try:
                os.remove(path)
            except FileNotFoundError:
                pass

        #Get MBTA bus GIS files
        BusGisURLs = {"BusRoutesAndStops":'http://wsgw.mass.gov/data/gispub/shape/state/mbtabus.zip'}
        BusGisKeys = {"BusRoutes":'MBTABUSROUTES_ARC', "BusStops":'MBTABUSSTOPS_PT'}
        BusdbfKeys = {"RoutesToStops":'MBTABUSSTOPS_PT_EVENTS.dbf'}

        for key in BusGisURLs:
            
            # Get the data from the server.
            response = urllib.request.urlopen(BusGisURLs[key])

            # Unzip the file into the working directory.
            zip_ref = zipfile.ZipFile(io.BytesIO(response.read()))
            zip_ref.extractall("./")
            zip_ref.close()

            # Function to reverse the Lambert projection done on the route/stop data and convert to latitude/longitude.
            reverseCoordinateProjection = pyproj.Proj(proj = 'lcc', datum = 'NAD83', lat_1 = 41.71666666666667, lat_2 = 42.68333333333333,lat_0 = 41.0, lon_0 = -71.5, x_0 = 200000.0, y_0 = 750000.0)

            # Read the MBTA Bus Routes file into the Python shapefile library.
            sfRoute = shapefile.Reader(BusGisKeys['BusRoutes'])

            # List comprehension to pull out route coordinates and related data in GeoJSON format.
            geoJSONRoutes = [
            {
                "type":"Feature",
                "geometry":{
                    "type":"Multipoint",
                    "coordinates": [(reverseCoordinateProjection(p[0], p[1], inverse = True)[1], reverseCoordinateProjection(p[0], p[1], inverse = True)[0]) for p in x.shape.points]},
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
            sfStops = shapefile.Reader(BusGisKeys['BusStops'])

            # List comprehension to pull out stop coordinates and related data in GeoJSON format.
            geoJSONStops = [
            {
                "type":"Feature",
                "geometry":{
                    "type":"Point",
                    "coordinates": (reverseCoordinateProjection(x.shape.points[0][0], x.shape.points[0][1], inverse = True)[1], reverseCoordinateProjection(x.shape.points[0][0], x.shape.points[0][1], inverse = True)[0]) },
                "properties":{
                    "stop_name": x.record[1],
                    "stop_id": x.record[0],
                    "town":x.record[2],
                    "route_list":[]
                }
            }
            for x in [i for i in sfStops.shapeRecords()]]

            # Read the MBTA Routes to Stops file into the Python dbf library.
            sfRTS = DBF(BusdbfKeys['RoutesToStops'], load=True)
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

            #Trial mode: run Algo R for 1000 samples
            if(trial == True):
                k = 1000
                sample = geoJSONRoutes[0:k]
                for i in range(2000,len(geoJSONRoutes)):
                    j = random.randint(0, i)
                    if(j <= k-1):
                        sample[j] = geoJSONRoutes[i]
                geoJSONRoutes = sample

            #Trial mode: run Algo R for 1000 samples
            if(trial == True):
                k = 1000
                sample = geoJSONStops[0:k]
                for i in range(2000,len(geoJSONStops)):
                    j = random.randint(0, i)
                    if(j <= k-1):
                        sample[j] = geoJSONStops[i]
                geoJSONStops = sample

            # Set up the database connection for routes
            repo.dropPermanent('BusRoutes')
            repo.createPermanent('BusRoutes')
            repo['alaw_markbest_tyroneh.BusRoutes'].insert_many(geoJSONRoutes)

            # Set up the database connection for stops
            repo.dropPermanent('BusStops')
            repo.createPermanent('BusStops')
            repo['alaw_markbest_tyroneh.BusStops'].insert_many(geoJSONStops)

        # Remove files after unzipping
        for path in ['MBTA_Bus_Routes_and_Stops_GDB.lyr','MBTA_Bus_Routes_and_Stops_SHP.lyr','MBTABUSSTOPS_PT_EVENTS.dbf','MBTABUSSTOPS_PT_EVENTS.dbf.xml',
        'MBTABUSROUTES_ARC_EVENTS.cpg','MBTABUSROUTES_ARC_EVENTS.dbf','MBTABUSROUTES_ARC_EVENTS.dbf.xml','MBTABUSROUTES_ARC.cpg','MBTABUSROUTES_ARC.dbf',
        'MBTABUSROUTES_ARC.prj','MBTABUSROUTES_ARC.sbn','MBTABUSROUTES_ARC.sbx','MBTABUSROUTES_ARC.shp','MBTABUSROUTES_ARC.shp.xml','MBTABUSROUTES_ARC.shx',
        'MBTABUSSTOPS_PT_EVENTS.cpg','MBTABUSSTOPS_PT.cpg','MBTABUSSTOPS_PT.dbf','MBTABUSSTOPS_PT.prj','MBTABUSSTOPS_PT.sbn','MBTABUSSTOPS_PT.sbx',
        'MBTABUSSTOPS_PT.shp','MBTABUSSTOPS_PT.shp.xml','MBTABUSSTOPS_PT.shx']:
            os.remove(path)

        shutil.rmtree('MBTA_BUS.gdb')    

        repo.logout()
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
        
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/') # Boston Data Portal
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/') # Cambridge Data Portal
        doc.add_namespace('sdp', 'https://data.somervillema.gov/resource/') # Somerville Data Portal
        doc.add_namespace('brdp', 'http://data.brooklinema.gov/datasets/') # Brookline Data Portal
        doc.add_namespace('hub', 'https://s3.amazonaws.com/hubway-data/') # Hubway Data 
        doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/routes/') # MBTA API
        doc.add_namespace('datm', 'http://datamechanics.io/data/') # datamechanics.io
        doc.add_namespace('gis', 'http://wsgw.mass.gov/data/gispub/shape/') # MassGIS 

        this_script = doc.agent('alg:alaw_markbest_tyroneh#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_BostonProperty = doc.entity('bdp:jsri-cpsq', {'prov:label':'Boston Properties Property Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_BostonProperty = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Boston Properties Property Coordinates'})
        doc.wasAssociatedWith(get_BostonProperty, this_script)
        doc.usage(get_BostonProperty, resource_BostonProperty, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?$select=location&$limit=11000000'
                }
            )
        
        resource_CambridgeProperty = doc.entity('cdp:ufnx-m9uc', {'prov:label':'Cambridge Properties Property Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_CambridgeProperty = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Cambridge Properties Property Coordinates'})
        doc.wasAssociatedWith(get_CambridgeProperty, this_script)
        doc.usage(get_CambridgeProperty, resource_CambridgeProperty, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'$limit=11000000'
                }
            )
            
        resource_SomervilleProperty = doc.entity('sdp:dhs3-5kuu', {'prov:label':'Somerville Properties Property Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_SomervilleProperty = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Somerville Properties Property Coordinates'})
        doc.wasAssociatedWith(get_SomervilleProperty, this_script)
        doc.usage(get_SomervilleProperty, resource_SomervilleProperty, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'$limit=11000000'
                }
            )
        
        resource_BrooklineProperty = doc.entity('brdp:a725742a993f425ea463c2c509d91ca3_1', {'prov:label':'Brookline Properties Property Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_BrooklineProperty = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Brookline Properties Property Coordinates'})
        doc.wasAssociatedWith(get_BrooklineProperty, this_script)
        doc.usage(get_BrooklineProperty, resource_BrooklineProperty, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':"$limit=11000000"
                }
            )  
        
        resource_HubwayStations = doc.entity('hub:2016_0429_Hubway_Stations', {'prov:label':'Hubway Stations Coordinates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        get_HubwayStations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Hubway Stations Coordinates'})
        doc.wasAssociatedWith(get_HubwayStations, this_script)
        doc.usage(get_HubwayStations, resource_HubwayStations, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':""
                }
            )
        
        resource_TCStops = doc.entity('mbta:wX9NwuHnZU2ToO7GmGR9uw', {'prov:label':'T and Commuter Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_TCStops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get T and Commuter Stations'})
        doc.wasAssociatedWith(get_TCStops, this_script)
        doc.usage(get_TCStops, resource_TCStops, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':""
                }
            )


        resource_TimedBuses = doc.entity('datm:alaw_markbest_tyroneh#busdata', {'prov:label':'Boston Bus Coordinates', prov.model.PROV_TYPE:'ont:DataReource', 'ont:Extension':'geojson'})
        get_TimedBuses = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Boston Bus Coordinates'})
        doc.wasAssociatedWith(get_TimedBuses, this_script);
        doc.usage(get_TimedBuses, resource_TimedBuses, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':""
                }
            )

        resource_CensusPopulation = doc.entity('gis:census2010/CENSUS2010TOWNS_SHP', {'prov:label':'Mass Census Data', prov.model.PROV_TYPE:'ont:DataReource', 'ont:Extension':'zip'})
        get_MassCensus = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Mass Census Data'})
        doc.wasAssociatedWith(get_MassCensus, this_script);
        doc.usage(get_MassCensus, resource_CensusPopulation, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':""
                }
            )

        resource_BusRoutes = doc.entity('gis:state/mbtabus', {'prov:label':'Mass Bus Route Data', prov.model.PROV_TYPE:'ont:DataReource', 'ont:Extension':'zip'})
        get_BusRoutes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Mass Bus Route Data'})
        doc.wasAssociatedWith(get_BusRoutes, this_script);
        doc.usage(get_BusRoutes, resource_BusRoutes, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':""
                }
            )

        resource_BusStops = doc.entity('gis:state/mbtabus', {'prov:label':'Mass Bus Stop Data', prov.model.PROV_TYPE:'ont:DataReource', 'ont:Extension':'zip'})
        get_BusStops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Get Mass Bus Route Data'})
        doc.wasAssociatedWith(get_BusStops, this_script);
        doc.usage(get_BusStops, resource_BusStops, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':""
                }
            )

        BostonProperty = doc.entity('dat:alaw_markbest_tyroneh#BostonProperty', {prov.model.PROV_LABEL:'Boston Properties Property Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(BostonProperty, this_script)
        doc.wasGeneratedBy(BostonProperty, get_BostonProperty, endTime)
        
        CambridgeProperty = doc.entity('dat:alaw_markbest_tyroneh#CambridgeProperty', {prov.model.PROV_LABEL:'Cambridge Property Property Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(CambridgeProperty, this_script)
        doc.wasGeneratedBy(CambridgeProperty, get_CambridgeProperty, endTime)
        
        SomervilleProperty = doc.entity('dat:alaw_markbest_tyroneh#SomervilleProperty', {prov.model.PROV_LABEL:'Somerville Property Property Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(SomervilleProperty, this_script)
        doc.wasGeneratedBy(SomervilleProperty, get_SomervilleProperty, endTime)
        
        BrooklineProperty = doc.entity('dat:alaw_markbest_tyroneh#BrooklineProperty', {prov.model.PROV_LABEL:'Brookline Property Property Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(BrooklineProperty, this_script)
        doc.wasGeneratedBy(BrooklineProperty, get_BrooklineProperty, endTime)
        
        HubwayStations = doc.entity('dat:alaw_markbest_tyroneh#HubwayStations', {prov.model.PROV_LABEL:'Hubway Stations Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(HubwayStations, this_script)
        doc.wasGeneratedBy(HubwayStations, get_HubwayStations, endTime)

        TCStops = doc.entity('dat:alaw_markbest_tyroneh#TCStops', {prov.model.PROV_LABEL:'T and Commuter Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(TCStops, this_script)
        doc.wasGeneratedBy(TCStops, get_TCStops, endTime)

        TimedBuses = doc.entity('dat:alaw_markbest_tyroneh#TimedBuses', {prov.model.PROV_LABEL:'Boston Bus Coordinates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(TimedBuses, this_script);
        doc.wasGeneratedBy(TimedBuses, get_TimedBuses, endTime);

        MassCensus = doc.entity('dat:alaw_markbest_tyroneh#CensusPopulation', {prov.model.PROV_LABEL:'Mass Census Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(MassCensus, this_script);
        doc.wasGeneratedBy(MassCensus, get_MassCensus, endTime);

        BusRoutes = doc.entity('dat:alaw_markbest_tyroneh#BusRoutes', {prov.model.PROV_LABEL:'Mass Bus Routes Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(BusRoutes, this_script);
        doc.wasGeneratedBy(BusRoutes, get_BusRoutes, endTime);

        BusStops = doc.entity('dat:alaw_markbest_tyroneh#BusStops', {prov.model.PROV_LABEL:'Mass Bus Stops Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(BusStops, this_script);
        doc.wasGeneratedBy(BusStops, get_BusStops, endTime);
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc
    
    def run(trial=False):
        '''
        Scrap datasets and write provenance files
        '''

        times = getData.execute(trial=trial)
        getData.provenance(startTime = times['start'], endTime = times['end'])

# if __name__ == '__main__':
#     getData.run()
## eof
