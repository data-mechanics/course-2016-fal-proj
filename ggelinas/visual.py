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

# Creates heatmap for crimes in Boston
heatmap = folium.Map(location=[42.359716, -71.065917], zoom_start=12)
heatmap.add_children(plugins.HeatMap([[c[1], c[0]] for c in [crime["location"]["coordinates"] for crime in repo['ggelinas.incidents'].find()]]))
heatmap.save("heatmap.html")


# Creates a map containing markers for hospitals and optimal hospitals
Hospital = []
Hospitalname = []
optHospital = []
for h in repo['ggelinas.hospitals'].find():
    Hospitalname.append(h['name'])
    Hospital.append((h['location']['coordinates'][1], h['location']['coordinates'][0]))

for o in repo['ggelinas.kmeanshospital'].find():
    optHospital.append((o['latitude'], o['longitude']))

hospitalmap = folium.Map(location=[42.355, -71.0609], zoom_start=13)
for i in range(len(Hospital)):
    lat, long = Hospital[i]
    name = Hospitalname[i]
    folium.CircleMarker(location=[lat, long], popup=name, color='#ff0000',  fill_color='#ff0000', radius=50, fill_opacity=0.7).add_to(hospitalmap)
for j in range(len(optHospital)):
    lat, long = optHospital[j]
    name = Hospitalname[j]
    folium.CircleMarker(location=[lat, long], popup='Optimal ' + name, color='#0000ff', fill_color='#0000ff', radius=50, fill_opacity=0.7).add_to(hospitalmap)
hospitalmap.save('hospitalmap.html')

NumCrimes = []
PropValue = []
for crime in repo['ggelinas.stations'].find().sort("location_zip", 1):
    NumCrimes.append(crime['num_crimes'])

count = 0
for value in repo['ggelinas.districtvalue'].find().sort("zip_code", 1):
    PropValue.append(value['avg_value'])

# Creates a correlation graph with crimes and property
districts = ['A1',  'D4', 'E13', 'B3', 'E18', 'D14', 'A7', 'C6', 'B2', 'E5', 'C11']
scat = plt.scatter(NumCrimes, PropValue, alpha=0.5)
plt.plot(NumCrimes, numpy.poly1d(numpy.polyfit(NumCrimes, PropValue, 1))(NumCrimes))
for i, txt in enumerate(districts):
    plt.annotate(txt, (NumCrimes[i], PropValue[i]))
plt.xlabel('Number of Crimes in Police District')
plt.ylabel('Average Property Value')
plt.title("Crime Rates and Average Property Value within Police Districts")
plt.ylim(0, 12000000)
plt.xlim(0, 180)

plt.savefig('crime_prop_corr.png')
plt.show()

endTime = datetime.datetime.now()

repo.logout()

