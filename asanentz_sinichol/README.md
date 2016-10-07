# Project Proposal



## The Question

How does traffic relate to alternate means of transportation and wealth?

To break it down:
- Are poorer areas getting the alternate transportation options that they need?
- Does a wealthier area have more vehicular traffic? 
- Do alternate means of transport assuage traffic?

To do so, we are comparing bus stop locations, hubway stop locations, and car charging stations with traffic and address data. We are looking at how many alternate transportation stops are available relative to an address, and comparing the information with vehicular traffic for the same address/area.



# The Data Sets

Master Address List (taken from Property Assessment : https://data.cityofboston.gov/Permitting/Property-Assessment-2016/i7w8-ure5

Hubway Stops: http://bostonopendata.boston.opendata.arcgis.com/datasets/ee7474e2a0aa45cbbdfe0b747a5eb032_4

Electric Car Charging Stations: http://bostonopendata.boston.opendata.arcgis.com/datasets/465e00f9632145a1ad645a27d27069b4_2

MBTA Bus Stops: http://bostonopendata.boston.opendata.arcgis.com/datasets/f1a43ad3c46b4ac89b74cdaba393ccac_4

WAZE Traffic Jam Data:	https://data.cityofboston.gov/Transportation/Waze-Jam-Data/yqgx-2ktq


# To Run

The files to access the data sets are:

#### addresses.py
#### busStops.py
#### chargingstations.py
#### hubway.py
#### traffic.py

# The combined datasets can be found in:

### addressValue.py 	

This file combines all bus stop and hubway data with a given address - we see how many of these stops/stations are present, relative to a given address.


### delayMap.py

This file combines delay time traffic-wise with a given address - we see how much delay time one can expect traffic-wise at a given address.


### transit.py

This file combines all hubway and bus stop data together into a single database.


# Auth

auth.json was formatted: 

{"cityofboston": "XxXXXXXxXxXxXXXXxxxxXxXX"}



## In order to run the files:

Run the five "access the data" sets first.

Then, run transit.py - which combines hubway and bus stop data together.

Then, you can run any of the other combination files.

The script to run any .py files:

$ python3 [file name].py


