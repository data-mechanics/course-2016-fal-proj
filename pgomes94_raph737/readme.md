Patrick Gomes and Raphael Baysa

We decided to look into the optimal area to build a new hospital in the Boston area. The data sets we are using include current hospital locations, police station locations, mbta/train stops, traffic points and crime rates. The new hopsital would be located far from current hospital locations and from police stations, but near high population density locations, preferably near two or more clusters. Accessibility is a concern so it would have to be close to a bus or train stop, but far away from high traffic areas for ambulances. If possible, the optimal hospital would also be located near, but not in, a crime cluster for faster response. 

---
Data Sets
---

Hospitals: https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz

MBTA/Train: http://realtime.mbta.com/developer/api/v2/stopsbylocation?api_key="+api_key+"&lat="+lat+"&lon="+lon+"&format=json

Traffic: https://data.cityofboston.gov/dataset/Waze-Point-Data/b38s-xmkq and https://data.cityofboston.gov/Transportation/Waze-Jam-Data/yqgx-2ktq

Crime: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap

Police Stations: https://data.cityofboston.gov/resource/pyxn-r3i2.json

---
Getting the data
___

Run the dataRequests.py to download all the data into the database with its appropriate transformations
