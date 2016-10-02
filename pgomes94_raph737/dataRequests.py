import requests, json,pprint

mbtaStops = requests.get("http://realtime.mbta.com/developer/api/v2/stopsbylocation?api_key=wX9NwuHnZU2ToO7GmGR9uw&lat=42.346961&lon=-71.076640&format=json")

data = mbtaStops.json()


pp = pprint.PrettyPrinter(indent=4)
pp.pprint(data)