## Relationship Between Subway and CitiBike Usage in New York City 
<br>
### Project 1: Data Retrieval, Storage, Provenance, and Transformations
___

#### Introduction
Is there a relationship between subway ridership and CitiBike usage in New York City?
This project aims to discover if there are patterns between these two major forms of 
commute. This could potentially help identify smart locations for bike hubs based on
proximity to subways. Our hypothesis is that it would be smarter to place more Bike Hubs
around overcrowded subway stations. In addition to comparing these forms of transport, 
this project will explore subway and CitiBike usage based on pedestrian traffic and weather
in the stations' respective regions. With this information, we can see where it would be
worth adding or removing CitiBike stations if there is a level of usage disproportional
to the pedestrian traffic, or during which months maintenance of these services is most 
important.

#### Datasets
1. [Subway Stations](https://data.cityofnewyork.us/Transportation/Subway-Stations/arq3-7z49)
2. [Bi-Annual Pedestrian Counts](https://data.cityofnewyork.us/Transportation/Bi-Annual-Pedestrian-Counts/2de2-6x2h) 
3. [Subway Turnstile Data](http://web.mta.info/developers/turnstile.html)
4. [CitiBike System Data](https://www.citibikenyc.com/system-data)
5. [Central Park Weather Data](https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00094728/detail)

#### Project Dependencies 
* python3.5+
* [dml](https://pypi.python.org/pypi/dml)
* [prov](https://pypi.python.org/pypi/prov)

#### Retrieve Data
1. Install project dependencies sand make sure you have ```retrievedata.py```, ```citizip_urls.txt```, and the three ```transformation[#].py``` (```[#]``` being 1, 2, 3) files in your working directory

2. Run MongoDB with authentication

3. Retrieve the five initial datasets: ```$ python3 retrieve_data.py```

#### Data Transformations
Now we can run the following transformations on the data: (```user``` refers to the MongoDB user):

**Transformation 1: Classifying subway station by NYC region** <br>
**File**: ```transformation1.py``` <br>
**Uses**: ```user.subway_stations```, ```user.pedestriancounts``` <br>
**Rationale**: The pedestrian counts in the [bi-annual pedestrian counts dataset](http:/www.nyc.gov/html/dot/downloads/pdf/bi-annual-ped-count-readme.pdf) is based on "114 
locations, including 100 on-street locations (primarily retail corridors), 13 East River and
Harlem River bridge locations, and the Hudson River Greenway." The subway dataset
contains coordinates of each subway station, but we wanted a way to classify each subway
by a rgion so we can eventually analyze each station's activity by foot traffic (pedestrian
count) in its region. The bi-annual pedestrian counts dataset has carefully selected locations
that are well-suited as standardized regions for measurement.<br>
**Creates**: ```user.subway_regions```<br>
**Activities:** Uses 2D-sphere to classify each subway station into a region in bi-annual pedestrian counts
dataset

**Transformation 2: Fix CitiBike station coordinates and classify by NYC region** <br>
**File**: ```transformation2.py``` <br>
**Uses**: ```user.citibike``` , ```user.pedstriancounts``` <br>
**Rationale**: In order to do interesting things with CitiBike station coordinates, we need to get the coordinates in a more standardized, usable form. Also, we want to classify each CitiBike station by region like we did with subway stations. <br>
**Creates**: ```user.citibike_by_region``` <br>
**Activities**: 

* location information and consolidate separate longitude/latitud fields to ```'the_geom' : {'type': 'Point', 'coordinates': [long, lat]}}``` format
* Uses 2D-sphere to classify each CitiBike station into a region in bi-annual pedestrian counts dataset

**Transformation 3: Add Weather fields to turnstile data** <br>
**File**: ```transformation3.py``` <br>
**Uses**: ```user.weather```, ```user.turnstile``` <br>
**Rationale**: Adding weather to daily turnstile data will give us context of how subway usage varies with weather <br>
**Creates**: ```user.turnstiles_and_weather``` <br>
**Activities**:

* Create new table joining relevant turnstile data and weather by date
* Corrects exits column label (gets rid of extraneous spaces) in dataset <br>


**Transformation 4: Add weather to CitiBike station data** <br>
**File**:  ```transformation4.py``` <br>
**Uses**:  ```user.weather```, ```user.citibike```<br>
**Creates**:  ```user.citibike_weather``` <br>

* Create new table joining relevant turnstile data and weather by date

**Transformation 5: Add weather to SubwayStation and Turnstiles <br>
**File**: ```transformation5.py```<br>
**Uses**: ```user.subway_stations```, ```user.turnstile```, ```user.weather```  <br>
**Creates**: ```user.turnstile_weather```, ```user.turnstile_station```  <br>


* Compute turnstile usage by day for each subway station
* Combine weather data with daily subway usage 

**Transformation 6: Add Pedestrian counts to citibike and subway stations** <br>
**File**:  ```transformation6.py``` <br>
**Uses**: ```user.subway_regions```, ```user.pedestriancounts```, ```user.citibike_by_region```<br>
**Creates**:  ```user.daily_pedestrian```, ```user.subway_pedestriancount```, ```user.citibike_pedestriancount```<br>
**Activities**:

* Count usage of citibike by day
* Count usage of subway stations by day 
* Combine counts of subway and citibike usage with the pedestrian regions 

**Transformation 7:  Get total CitiBike usage and weather for each day** <br>



#### Considerations and Limitations
* To ensure all stations had a region classification, we had to use a 8000m radius (~5miles). While most subway stations were within a much shorter distance from a region, we realize this radius may be too large to properly classify other stations.
* This project topic will continue to be refined as the project progresses and more insights are discovered.

<br><br>
### Project 2: Modelling, Optimization, and Statistical Analysis
___

#### Narrative 
With subway usage, citibike usage, annual weather, and regional pedestrian counts, we can figure out the relationship between these variables using statistical methods such as correlation and regression. With our datasets, we will see how Citibike and subway usage varies with weather, as well as how they are affected by the region's population/pedestrian traffic.

We aim to answer the following family of problems:

1. **How does CitiBike/subway usage vary with weather? Do riders prefer one over the other in certain weather conditions?**
	* Hypotheses:
		* There is a positive correlation between precipitation and subway ridership
		* 	There is slight positive correlation between temperature and subway ridership
		* 	There is a negative correlation between precipitation and CitiBike ridership
		* There is a positive correlation between temperature and CitiBike ridership 

	We can solve this problem by combining subway usage, CitiBike usage, and weather by date. 	Once we have the total subway usage and totaly CitiBike usage for each day, we can find the 	correlation between each of these usages and weather. 

2. **Can we predict subway usage from pedestrian count of a region using a linear regression model?** 

#### Statistical Analysis Results

For statistical analysis #1, we found the folllwing results: 
* Correlation between precipitation and subway usage is -0.0781896796422 with a p-value of 0.0478418069172
* Correlation between temperature and subway usage is 0.0205902690486 with a p-value of 0.602825476302
* Correlation between precipitation and citibike usage is **-0.252789181096** with a p-value of **1.05252365339e-14**
* Correlation between temperature and citibike usage is **0.759934770081** with a p-value of **1.09959733967e-171 **

It is apparent that citibike usage is much more sensitive to weather than subway usage. We expected the negative and positive correlations between CitiBike usage and precipitation/temperature respectively. These correlations make sense because New Yorkers take the subway no matter what conditions. Therefore, there wouldn't be much correlation between weather 

For statistical analysis #2, we got the following results from the regression on subway usage based on pedestrian count: 

slope is 18.9080077596<br>
intercept is 517937.435161<br>
r-squared is 0.0854615984247<br>
Our regression equation is : Subway_Usage = 517937 + 18.9*(Region_Pedestrian_Count)

<!--
* **If we had a budget reduction and had to remove one station, which station would be the best choice to remove?**
	* Approach: Assume that the  optimal chioce of station to remove would be the station that has the lowest relative usage compared to the pedestrian count of the region in which the station is located. 
	* State space: *2^N*, where *N* is the number of stations
	* For each of the N stations, 0 signifies not removing and 1 signifies removing the station
		* Constraint: Choose 1 station to remove (i.e. should be only a single 1 in a permutation)
	* If the ratio *r* = (station usage / pedestrian count) for the n-th station, *s* is the choice to remove or keep the n-th station (0 or 1), and each list [ ... ] represents a possbile permutation, then our objective function *f*  is as follows:<br>
		sum( [ (s1 * r1), (s2 * r2), .... , (sn * rn) ] , [ ... ], [ ... ] )<br>
	* We're looking to minimize this objective function: <br>
		*argmin s ∈ S f(s)* 


___
#### TODO
* change retrieval algorithm to get subway_stations from datamechanics.io repository
* Fix prov mistake in Project 1, Problem 3. (25 pts) 20/ 25: <br>
           (1) In transformation provenance()
             functions, you use the "cny" namespace
             for your own data sets; it should be "dat". <br>
           (2) The provenance information is excessive;
             retrieving your own data set does not need
             to be its own activity in every transformations.
             It is enough for the transformation itself
             to correspond to one activity. See the alice_bob
             example. 

#### Tasks <br>

* At least three Non-trivial Problem-solving methods:
	* Statistical Analysis
		* Subway usage vs citibike usage based on weather/season
		* ratios by region: (station usage / pedestrian count) and (citibike usage / pedestrian count). compare and see where there is an excessive ratio of pedestrians to station usage. more stations could be added here

	* Optimization
		* something about distance from closest subway for each citibike station

* Provenance for scripts/algorithms (do this correctly!)
-->

