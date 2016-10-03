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

routesA = [d for d in r["mode"] if d["mode_name"] == "Subway" or d["mode_name"] == "Commuter Rail"]
routesB = [route for d in routesA for route in d['route']]

stop_base = "http://realtime.mbta.com/developer/api/v2/stopsbyroute?api_key=wX9NwuHnZU2ToO7GmGR9uw"
stop_urls = {route["route_id"]:"{}&route={}&format=json".format(stop_base, route["route_id"]) for route in routesB}
responses = {route:urllib.request.urlopen(stop_urls[route]).read().decode("utf-8") for route in stop_urls}
json_stops = {route:json.loads(responses[route]) for route in responses}
#stops_dumps = {route:json.dumps(json_stops[route], sort_keys=True, indent=2) for route in json_stops}
stops_dumps = json.dumps(json_stops, sort_keys=True, indent=2)

result = []
for key in json_stops:
	result.append({key:json_stops[key]})