# Project Proposal



## The Question

Simply put, does public transit correlate to average income? In more words, do poorer areas (in terms of per capita income) have fewer means of transport in the form of bus, subway, and Hubway stops, and do richer areas have more?

To answer the question, we first create a grid along coarse (~1.1km) longitudinal and latitudinal lines, and add up the total number of bus/subway/Hubway stops in these blocks. We then assign these values to each of the houses in these blocks. We use this data to determine if each house has transit in a constraint satisfaction problem. Mattapan and Dorchester have clusters of houses without transit within 1.1km. Our k-means algorithm places two points in these areas.

We then take each of the towns in Boston (or as many as we can) and assign each their per capita incomes (or as many as we can). This data will be used to determine if the clusters of no transit are low-income areas (they are).



## The Data Sets

Master Address List (taken from Property Assessment : https://data.cityofboston.gov/Permitting/Property-Assessment-2016/i7w8-ure5

Hubway Stops: http://bostonopendata.boston.opendata.arcgis.com/datasets/ee7474e2a0aa45cbbdfe0b747a5eb032_4

MBTA Bus Stops: http://bostonopendata.boston.opendata.arcgis.com/datasets/f1a43ad3c46b4ac89b74cdaba393ccac_4

MBTA Subway Stops: https://www.mbta.com/uploadedfiles/MBTA_GTFS.zip

Income Data Per Capita (This dataset was later updated to add new income data):   https://github.com/Data-Mechanics/course-2016-spr-proj/blob/222e4fd34ad436932b58c44cdf8b31c2e9da27c4/jlam17_mckay678/data/Boston_IncomePerCapita.json


## Files that get data

The files to access the data sets are:

#### addresses.py
#### busStops.py
#### MBTAStops.py
#### hubway.py
#### income.py

See the "**In order to run the files**" section for instructions on how to run code

# The combined datasets can be found in:

### addressValue.py 	

This file combines all bus stop and hubway data with a given address - we see how many of these stops/stations are present, relative to a given address.

### transit.py

This file combines all hubway and bus stop data together into a single database.

### neighborhoodZipCodes.py

This file uses the addresses and income repos to get a list of zip codes, assign them to a neighborhood name, and then assign the zip codes/neighborhoods an income.

### constraintSatisfaction.py

This file creates a repo of all the houses without any buses, subways, or Hubways.

#### kmeans.py

This file uses the repo from constraintSatisfaction.py and uses a k-means algorithm to determine where 4 stops (of any of the three transport systems) should be placed. To see how we determined how many clusters, uncommment "evaluate_clusters(latlong,8)". To see the centers of the clusters, uncomment "print(centers)".

## Auth

auth.json is formatted as follows: 

{"cityofboston": "XxXXXXXxXxXxXXXXxxxxXxXX"}



# In order to run the files:

Run the five "access the data" sets first, in any order:
- addresses.py
- busStops.py
- MBTAStops.py
- hubway.py
- income.py

Then, in order:
- transit.py
- addressValue.py
- neighborhoodZipCodes.py
- constraintSatisfaction.py
- kmeans.py

The script to run any .py files:

$ python3 [file name].py


