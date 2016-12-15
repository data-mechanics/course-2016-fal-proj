import urllib.request
import urllib.parse
import json
import dml
import prov.model
import datetime
import uuid
import folium
import numpy
import matplotlib.pyplot as plt
from folium import plugins

import os, time


client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ggelinas', 'ggelinas')

startTime = datetime.datetime.now()

heatmap = folium.Map(location=[42.359716, -71.065917], zoom_start=12)
heatmap.add_children(plugins.HeatMap([[c[1], c[0]] for c in [crime["location"]["coordinates"] for crime in repo['ggelinas.incidents'].find()]]))
heatmap.save("heatmap.html")

# districts = ['A1',  'D4', 'E13', 'B3', 'E18', 'D14', 'A7', 'C6', 'B2', 'E5', 'C11']
# scat = plt.scatter(NumCrimes, PropValue, alpha=0.5)
# plt.plot(NumCrimes, numpy.poly1d(numpy.polyfit(NumCrimes, PropValue, 1))(NumCrimes))
# for i, txt in enumerate(districts):
#     plt.annotate(txt, (NumCrimes[i], PropValue[i]))
# plt.xlabel('Number of Crimes in Police District')
# plt.ylabel('Average Property Value')
# plt.title("Crime Rates and Average Property Value within Police Districts")
# plt.ylim(0, 12000000)
# plt.xlim(0, 180)
#
# plt.show()
# plt.savefig('crime_prop_corr.png')



endTime = datetime.datetime.now()

repo.logout()

