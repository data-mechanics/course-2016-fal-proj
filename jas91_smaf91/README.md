# Data Mechanics

## Project 1

### 2.a

We chose to combine datasets holding the following data: [Crime], [Schools], [Hospitals], [Food Establishment Inspections], [311] reports. An interesting project would be to rate a given zipcode based on the quality of it’s surroundings. 

### 2.b

The algorithm that retrieve these datasets automatically may be found in the file: ```get_data.py```.To run it:
```
>>> python3 get_data.py
```

### 2.c

The following transformations must be executed in order.

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

## Project 2

### Problem 1

Following the same idea as **Project 1** the goal is to rank the zipcodes according to the information we have so far. That is, [Crime], [Schools], [Hospitals], [Food Establishment Inspections], [311] reports. Having these data we can derive a new dataset with the following structure:

```
(zipcode, #crimes, #311 reports, #passed food inspectios, #schools, #hospitals)
```

A user might want to query this dataset in order to know which zipcode to choose to live in, based on the attributes mentioned above. The objective in this case, would be to minimize the ```#crimes``` and ```#311 reports```, while maximizing the quelity of the surrounding restaurants, that is, the ```#passed food inspections```, ```#schools``` and ```#hospitals```. Given equally importance to all five attributes.

This can be computed optimally using **skyline queries**. Where the result of the query will be formed of all non-dominated tuples following the *pareto optimality* definition [1]. Where an element *a = (a<sub>1</sub>, ..., a<sub>n</sub>)* dominates an element *b = (b<sub>1</sub>, ..., b<sub>n</sub>)* if:

for all *i* in {*1, ..., n*}, a<sub>i</sub> ≥ b<sub>i</sub> and exists *j* in {*1, ..., n*}, such as a<sub>j</sub> > b<sub>j</sub>

Then, the skyline set is defined by all the elements in the datsets that are not dominated by any other element.

### Problem 2

Given the crimes dataset, another interesting problem would be to find the minimum number of police patrols and where should these patrols located in order to maximize the coverage area of zones with high crime rate. At first, this would be a constraint satisfaction problem, that can be modeled as a **linear program**, and the results would be ```k```. Later this ```k``` can be used in order to find the specific locations where these patrols must be placed using **k-means**

## References

[1] U. Guntzer W.T. Balke. *Multi-objective query processing for database systems*. 2004
[Crime]: <https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx>
[Schools]: <https://data.cityofboston.gov/Facilities/School-Gardens/cxb7-aa9j>
[Hospitals]: <https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz>
[Food Establishment Inspections]: <https://data.cityofboston.gov/Health/Food-Establishment-Inspections/qndu-wx8w>
[311]: <https://data.cityofboston.gov/City-Services/311-Open-Service-Requests/rtbk-4hc4>
