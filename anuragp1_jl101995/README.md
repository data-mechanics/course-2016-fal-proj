# Project 1: Relationship Between Subway and CitiBike Usage in New York City 

## Introduction
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

## Datasets
1. [Subway Stations](https://data.cityofnewyork.us/Transportation/Subway-Stations/arq3-7z49)
2. [Bi-Annual Pedestrian Counts](https://data.cityofnewyork.us/Transportation/Bi-Annual-Pedestrian-Counts/2de2-6x2h) 
3. [Subway Turnstile Data](http://web.mta.info/developers/turnstile.html)
4. [CitiBike System Data](https://www.citibikenyc.com/system-data)
5. [Central Park Weather Data](https://www.ncdc.noaa.gov/cdo-web/datasets/GHCND/stations/GHCND:USW00094728/detail)

## Project Dependencies 
* python3.5+
* [dml](https://pypi.python.org/pypi/dml)
* [prov](https://pypi.python.org/pypi/prov)

## Retrieve Data
1. Install project dependencies sand make sure you have ```retrievedata.py```, ```citizip_urls.txt```, and the three ```transformation[#].py``` (```[#]``` being 1, 2, 3) files in your working directory

2. Run MongoDB with authentication

3. Retrieve the five initial datasets: ```$ python3 retrieve_data.py```

## Data Transformations
Now we can run the following transformations on the data: (```user``` refers to the MongoDB user):

* Transformation 1: Classifying subway station by NYC region 
File: ```transformation1.py```
Uses: ```user.subway_stations```, ```user.pedestriancounts```  
Rationale: The pedestrian counts in the [bi-annual pedestrian counts dataset](http://www.nyc.gov/html/dot/downloads/pdf/bi-annual-ped-count-readme.pdf) is based on "114 locations, including 100 on-street locations (primarily retail corridors), 13 East River and Harlem River bridge locations, and the Hudson River Greenway." The subway dataset contains coordinates of each subway station, but we wanted a way to classify each subway by a region so we can eventually analyze each station's activity by foot traffic (pedestrian count) in its region. The bi-annual pedestrian counts dataset has carefully selected locations that are well-suited as standardized regions for measurement.
Creates: ```user.subway_regions```
	* Uses 2D-sphere to classify each subway station into a region in bi-annual pedestrian counts dataset

* Transformation 2: Fix CitiBike station coordinates and add classify by NYC region 
File: ```transformation2.py```
Uses: ```user.citibike``` , ```user.pedstriancounts```
Rationale: In order to do interesting things with CitiBike station coordinates, we need to get the coordinates in a more standardized, usable form. Also, we want to classify each CitiBike station by region like we did with subway stations.
Creates: ```user.citibike_by_region```
	* Reformat location information and consolidate separate longitude/latitud fields to ```'the_geom' : {'type': 'Point', 'coordinates': [long, lat]}}``` format
	* Uses 2D-sphere to classify each CitiBike station into a region in bi-annual pedestrian counts dataset

* Transformation 3: Add Weather fields to turnstile data
File: ```transformation3.py```
Uses: ```user.weather```, ```user.turnstile```
Rationale: Adding weather to daily turnstile data will give us context of how subway usage varies with weather
Creates: ```user.turnstiles_and_weather```
	* Create new table joining relevant turnstile data and weather by date
	* Corrects exits column label (gets rid of extraneous spaces) in dataset


<!--TODO: Transformation 4: Get Turnstile Coordinates
File: ```transformation4.py```
Collections Used: ```user.turnstile, user.subway_stations``` 
Rationale:
Creates: -->

## Considerations and Limitations
* To ensure all stations had a region classification, we had to use a 8000m radius (~5miles). While most subway stations were within a much shorter distance from a region, we realize this radius may be too large to properly classify other stations.
* This project topic will continue to be refined as the project progresses and more insights are discovered.



