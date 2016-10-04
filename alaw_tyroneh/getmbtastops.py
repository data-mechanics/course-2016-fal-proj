import json
import urllib.request
#import dml
#import prov.model
import datetime

# Retrieve list of all subway routes
route_url = "http://realtime.mbta.com/developer/api/v2/routes?api_key=wX9NwuHnZU2ToO7GmGR9uw&format=json"

response = urllib.request.urlopen(route_url).read().decode("utf-8")
r = json.loads(response)
s = json.dumps(r, sort_keys=True, indent=2)

routesA = [mode for mode in r["mode"] if mode["mode_name"] == "Subway" or mode["mode_name"] == "Commuter Rail"]
'''
routesB = []
for mode in routesA:
    for route in mode['route']:
        route["mode_name"] = mode["mode_name"]
        routesB.append(route) 
'''
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
