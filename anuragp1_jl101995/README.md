## Exploring New York City Transit
### The Relationship Between Subway Usage and CitiBike Usage in NYC
Anurag Prasad (```anuragp1@bu.edu```) and Jarrod Lewis (```jl101995@bu.edu```)

#### Part I: Data Retrieval, Storage, Provenance, and Transformations

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
* python3+
* [dml](https://pypi.python.org/pypi/dml)
* [prov](https://pypi.python.org/pypi/prov)

#### Retrieve Data
1. Install project dependencies sand make sure you have ```retrievedata.py```, ```citizip_urls.txt```, and the three ```transformation[#].py``` (```[#]``` being 1, 2, 3) files in your working directory

2. Run MongoDB with authentication

3. Retrieve the five initial datasets: ```$ python3 retrieve_data.py```

#### Data Transformations
Now we can run the following transformations on the data: (```user``` refers to the MongoDB user):

**Transformation 1: Classify Subway Station by NYC Region**
**File**: ```transform_subway_region.py``` 
**Uses**: ```user.subway_stations```, ```user.pedestriancounts``` 
**Rationale**: The pedestrian counts in the [bi-annual pedestrian counts dataset](http:/www.nyc.gov/html/dot/downloads/pdf/bi-annual-ped-count-readme.pdf) is based on "114 
locations, including 100 on-street locations (primarily retail corridors), 13 East River and
Harlem River bridge locations, and the Hudson River Greenway." The subway dataset
contains coordinates of each subway station, but we wanted a way to classify each subway
by a rgion so we can eventually analyze each station's activity by foot traffic (pedestrian
count) in its region. The bi-annual pedestrian counts dataset has carefully selected locations
that are well-suited as standardized regions for measurement.
**Creates**: ```user.subway_regions```
**Activities:**

* Uses 2D-sphere to classify each subway station into a region in bi-annual pedestrian counts
dataset

**Transformation 2: Fix CitiBike Station Coordinates and Classify by NYC Region** 
**File**: ```transform_citibike_loc.py``` 
**Uses**: ```user.citibike``` , ```user.pedstriancounts``` 
**Rationale**: In order to do interesting things with CitiBike station coordinates, we need to get the coordinates in a more standardized, usable form. Also, we want to classify each CitiBike station by region like we did with subway stations. 
**Creates**: ```user.citibikecoordinates```, ```user.citibike_by_region``` 
**Activities**: 

* Get location information and consolidate separate longitude/latitude fields to ```'the_geom' : {'type': 'Point', 'coordinates': [long, lat]}}``` format
* Uses 2D-sphere to classify each CitiBike station into a region in bi-annual pedestrian counts dataset

**Transformation 3: Get Total Subway Usage and Weather for Each Day** 
**File**: ```transform_turnstile_weather.py```
**Uses**: ```user.subway_stations```, ```user.turnstile```, ```user.weather```  
**Rationale**:  To see the relationship between daily turnstile usage and weather.
**Creates**: ```user.turnstile_weather```, ```turnstile_total_byday```
**Activities**:

* Compute turnstile usage by day
* Combine weather data with daily subway usage 

**Transformation 4:  Get Total CitiBike Usage by Day and Weather for Each Day** 
**File**:  ```transform_citibike_weather.py``` 
**Uses**:  ```user.citibike```, ```user.weather```
**Rationale**: To see the relationship between daily CitiBike usage and weather.
**Creates**:  ```user.citibike_weather``` 
**Activities**: 

* Compute CitiBike usage by day
* Combine weather data with daily citibike usage 

**Transformation 5: Add Pedestrian Counts to Citibike and Subway Stations** 
**File**:  ```transform_citibike_pedestrian.py``` 
**Uses**: ```user.subway_regions```, ```user.pedestriancounts```, ```user.citibike_pedestrian.```
**Rationale**: To analyze the impact of pedestrian traffic on transit usage, we must link stations with their respective region's pedestrian counts
**Creates**:  ```user.daily_pedestrian```, ```user.subway_pedestriancount```, ```user.citibike_pedestriancount```
**Activities**:

* Compute daily average pedestrian count for each of the 114 pedestrian count regions
* Count usage of CitiBike stations by day and combine with pedestrian count for the station's corresponding region
* Count usage of subway stations by day and combine with pedestrian count for the station's corresponding region

**Transformation 6: Creates Three JSON Files for D3 Map Visualization**
**File**: ```transform_pedestrian_coordinates.py```
**Uses**: ```anuragp1_jl101995.pedestriancounts```,```anuragp1_jl101995.daily_pedestrian```, ```anuragp1_jl101995.citibike```, 
          ```anuragp1_jl101995.turnstile_total_byday```, ```anuragp1_jl101995.subway_pedestriancount```', ```anuragp1_jl101995.subway_stations```]
**Rationale**: To create a D3 visualization of subway station, CitiBike station, and pedestrian region on a map with their usages, it is necessary to get each station with their coordinates and usage in JSON format.
**Creates**: ```anuragp1_jl101995.citi_coord_json```, ```anuragp1_jl101995.ped_coord_json```, ```anuragp1_jl101995.subway_coord_json```
**Activities**:

* Get subway station coordinates and their usages 
* Get CitiBike station coordinates and their usages
* Get pedestrian region coordinates and their pedestrian counts 
* NOTE: The above code performs the original transformations and creates the necesary JSON files, but the following code loads in the cleaned JSON files that are needed for D3 map visualization

**Transformation 7: Get CitiBike Usage by Day**
**File**: ```
**Uses**:
**Rationale**:
**Creates**:
**Activities**:

**Transformation 8: Create CSV with All CitiBike/Subway Stations and Their Usage by Day, Calculate Correlation Between Aggregate Subway and CitiBike Usage, and Plot**
**File**: ```transform_byday.py```
**Uses**:
**Rationale**:
**Creates**:
**Activities**:

**Transformation 9: Plot Effect of Weather on CitiBike/Subway Usage**
**File**: ```transform_plot_weather.py```
**Uses**:
**Rationale**:
**Creates**:
**Activities**:


#### Considerations and Limitations
* To ensure all stations had a region classification, we had to use a 8000m radius (~5miles). While most subway stations were within a much shorter distance from a region, we realize this radius may be too large to properly classify other stations.
* This project topic will continue to be refined as the project progresses and more insights are discovered.

--- 
### Part II: Modelling, Optimization, and Statistical Analysis

#### Narrative 
With subway usage, citibike usage, annual weather, and regional pedestrian counts, we can figure out the relationship between these variables using statistical methods such as correlation and regression. With our datasets, we will see how Citibike and subway usage varies with weather, as well as how they are affected by the region's population/pedestrian traffic.

We aim to answer the following family of problems:

1. **How does CitiBike/subway usage vary with weather? Do riders prefer one over the other in certain weather conditions?**
	* Hypotheses:
		* There is a positive correlation between precipitation and subway ridership
		* 	There is slight positive correlation between temperature and subway ridership
		* 	There is a negative correlation between precipitation and CitiBike ridership
		* There is a positive correlation between temperature and CitiBike ridership 

We can solve this problem by combining subway usage, CitiBike usage, and weather by date. 	Once we have the total subway usage and totaly CitiBike usage for each day, we can find the 	correlation between each of these usages and weather. Computations for this are contained in ```corr_weather.py```.

<br>
2. **Can we predict subway usage from pedestrian count of a region using a linear regression model?** 

```regression.py``` contains the linear regression calculation.

#### Statistical Analysis Results

##### Statistical Analysis 1: Weather and Station Usage
We found the folllwing results:

* Correlation between precipitation and subway usage is -0.0781896796422 with a p-value of 0.0478418069172
* Correlation between temperature and subway usage is 0.0205902690486 with a p-value of 0.602825476302
* Correlation between precipitation and citibike usage is **-0.252789181096** with a p-value of **1.05252365339e-14**
* Correlation between temperature and citibike usage is **0.759934770081** with a p-value of **1.09959733967e-171**

It is apparent that citibike usage is much more sensitive to weather than subway usage. We expected the negative and positive correlations between CitiBike usage and precipitation/temperature respectively. These correlations make sense because New Yorkers take the subway no matter what conditions. Therefore, there wouldn't be much correlation between weather 

##### Statistical Analysis 2: Regression of Pedestrian Count on Subway Usage
After conducting a linear regression of region pedestrian count on the subway station entries, we generated the following regression parameters:

slope is 18.9080077596<br>	
intercept is 517937.435161<br>
r-squared is 0.0854615984247<br>

This yields the regression equation ```Subway_Usage = 517937 + 18.9*(Region_Pedestrian_Count)```. Unfortunately the r-squared value indicates that the variation in pedestrian count by region *cannot* explain the variation in subway usage. This is a surprising finding for us because we expected subway station usage would be heavily effected by the pedestrian traffic in that station's region.
 
##### Statistical Analysis 3: Correlation between Aggregate Daily Subway Usage and CitiBike Usage


--- 
### Part III: Visualizations, Web Services, and Project Completion

1. Extend your project with two new features/components, where each feature/component is either an interactive web-based visualization that can be displayed in a standard web browser or a web service with a RESTful web API. You might choose to create two visualizations given the results you produced a previous project, or you might choose to create an interactive client-server (i.e., web interface and web service) application that allows users to invoke an algorithm or statistical analysis using their own specific parameters.


##### Visualization 1: Station Usage Timeseries
--> update to make filter by something else:
	* by clusters of stations by k-means
	* by borough 

##### Visualization 2: Effect of Weather

##### Visualization 3: Mapped Stations

##### Visualization 4: Optimization?


2. Complete a well-written and thorough description of your project (and the components you implemented) in the form of a report (which can be a README.md file within your repository, though HTML or PDF are also acceptable). It is expected that the report should come out to at least 3-5 pages (if printed in a 12-point font on 8.5 by 11 in. sheets), but there's no upper limit on length.
* The report should bring together all the text, diagrams, and images you generated and put them together into a coherent summary that explains what you did for someone who is not familiar with your project. Some ideas for what you may want to include (this is a rough outline in the form of introduction, methods, results, and future work, with suggested subtopics that may or may not apply in your case, so adjust or substitute as appropriate).
	* an introduction/narrative describing the problem or topic you chose to address and any associated motivation or background:
	* list the data sets you used or other online resources you collected or retrieved automatically, and their origins, and
	* describe any data sets you assembled manually or programmatically using the above;
	* specify which algorithms, analysis techniques, or tools you used, and describe any interesting issues you encountered with regard to:
	* usability and/or performance of the tool or technique,
	* necessary adjustments or transformations to the data in order to make it compatible with the tool or technique, and/or
	* limitations of the technique or the data;
summarize the results and your conclusions about the problem or topic (whether or not they are definitive or open problems remain), and include screenshots of the visualizations (or the visualizations themselves);
describe any ideas for future work that you had while working on the project that you think might be useful to pursue, or that you did not have a chance to pursue (this will be particularly useful to us for the next iteration of the course).


<!--
#### TODO: Analyses, Problem Solving, Visualizations
* **Statistical analysis supporting our map visualization: correlation between subway and citibike usage (in same geographic area))**
* Update timeseries visualization:
	* by borough filtering
* K-means algorithm since we have coordinates of citibike and turnstiles now?
* Something with citibike trip paths? (are people's trips going *toward subways*?)
* Update Optimization: **If we had a budget reduction and had to remove a CitiBike station, which station would be the best choice to remove?**
	* Approach: Assume that the  optimal chioce of station to remove would be the station that (1) within the same [?] meter radius of a subway and (2) has the lowest relative usage of those that satisfy (1). 
	* State space: *2^N*, where *N* is the number of stations
	* For each of the N stations, 0 signifies not removing and 1 signifies removing the station
		* Constraint: Choose 1 CitiBike station to remove (i.e. should be only a single 1 in a permutation)
	* If *d* = distance from closest subway station for the n-th CitiBike station, *c* is the choice to remove or keep the n-th station (0 or 1), and each list [ ... ] represents a possbile permutation of CitiBike statioins within a [?] meter radius of a subway station, then our objective function *f*  is as follows:<br>
		sum( [ (c1 * d1), (c2 * d2), .... , (cn * dn) ] , [ ... ], [ ... ] )<br>
	* We're looking to minimize this objective function: <br>
		*argmin s ∈ S f(s)* 

#### TODO: Transformations, sources, and provenance 
* Change jupyter notebook files to their own transformations
* Make byday_tocsv generate csv in appropriate folder (the visualizations/usage_vis/ directory) and then perform transformation
* CLEAN CODE, FIX ALL PROVENANCE, AND PERFECT README
* Get everything to run seamlessly through the database without manual changes and physical files

-->

