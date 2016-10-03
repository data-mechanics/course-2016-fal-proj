#2a


###Datasets used:

'Boston Police Stations': 'https://data.cityofboston.gov/resource/pyxn-r3i2.json',

'Boston Crime Reports': 'https://data.cityofboston.gov/resource/29yf-ye7n.json',

'Boston Property Assessments':'https://data.cityofboston.gov/resource/g5b5-xrwi.json',

'Boston Field Interrogation and Observation':'https://data.cityofboston.gov/resource/2pem-965w.json',

'Boston Hospital Locations': 'https://data.cityofboston.gov/resource/u6fv-m8v4.json'

We also used the Google Geocoding API, the documentation of which can be found here: https://developers.google.com/maps/documentation/geocoding/intro

#2b

Run getData.py to retrieve all of the necessary data

#2c

###Non-trivial Algorithms implemented (Please run these in order!!!)

####getFIOcoord.py

>This method takes Field Interrogation and Observation data, which usually only contains a street address
with no city, state, zipcode, or coordinates, and retrieves the coordinates from the
Google Geocoding API. Although this only uses one dataset, we considered it to be 
non-trivial because it uses an external API.

####alg1.py (geojson encoder!)

>This method takes whatever location data is associated with the entries in all 5 datasets
and encodes it in the GeoJSON schema.

####numOfCrimeInDistricts.py

>This method compares data from the Police Incident Reports, and counts the number of crimes committed in each
district (each police station has its own district). It adds this count to the "stations" collection in the database
under the key "num_crimes"

#3
