# Project 1

## 2.a

We chose to combine datasets holding the following data: [Crime], [Schools], [Hospitals], [Food Establishment Inspections], [311] reports. An interesting project would be to rate a given zipcode based on the quality of it’s surroundings. 

## 2.b

The algorithm that retrieve these datasets automatically may be found in the file: ```get_data.py```.To run it:
```
>>> python3 get_data.py
```

## 2.c

#### Transformation 1

The first transformation has the objective to standarize the geographic information of the datasets. The GeoJSON format was used in the following way:
```
geo_info: {
    type: 'Feature',
    properties: {
        zip_code: ZIPCODE
    },
    geometry: {
        type: 'Point',
        coordinates: [LATITUDE, LONGITUDE]
    }
}
```
This script may be found in ```transformation1.py```. To run it:
```
>>> python3 transformation1.py
```

#### Transformation 2

The purpose of the second transformation is to populate the zip_code field of the crime dataset. Based on the information from the other datasets, it is possible to build and index. Later, given two coordinates from each crime entry find an entry within 1 Km range and assign its zip code. This script may be found in ```transformation2.py```. To run it:
```
>>> python3 transformation2.py
```

#### Transformation 3

Finally, in the third transformation, the amount od crimes and 311 service reports are grouped per zip code. This script may be found in ```transformation3.py```. To run it:
```
>>> python3 transformation3.py
```

[Crime]: <https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx>
[Schools]: <https://data.cityofboston.gov/Facilities/School-Gardens/cxb7-aa9j>
[Hospitals]: <https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz>
[Food Establishment Inspections]: <https://data.cityofboston.gov/Health/Food-Establishment-Inspections/qndu-wx8w>
[311]: <https://data.cityofboston.gov/City-Services/311-Open-Service-Requests/rtbk-4hc4>


