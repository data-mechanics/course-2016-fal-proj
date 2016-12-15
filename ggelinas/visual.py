import urllib.request
import urllib.parse
import json
import dml
import prov.model
import datetime
import uuid
import folium
from folium import plugins

import os, time


client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ggelinas', 'ggelinas')

startTime = datetime.datetime.now()

heatmap = folium.Map(location=[42.359716, -71.065917], zoom_start=12)
heatmap.add_children(plugins.HeatMap([[c[1], c[0]] for c in [crime["location"]["coordinates"] for crime in repo['ggelinas.incidents'].find()]]))
heatmap.save("heatmap.html")
display(heatmap)

endTime = datetime.datetime.now()

repo.logout()

