# course-2016-fal-proj (aliyevaa, bsowens, dwangus, jgtsui)
Project repository for the course project in the Fall 2016 iteration of the Data Mechanics course at Boston University.

## Justification

The data sets we have chosen are:

- Crime Incident Reports (from July 2012 - August 2015)
- Public Access Fishing Locations, Issued Moving Truck Permits
- Active Food Establishment Licenses, Entertainment Licenses
- Community Supported Agriculture Pickups
- Year-Round Swimming Pools. (will be cleaned up for Proj-3)
- Boston Parking Lots
- Boston Libaries

The first five datasets are pulled from [City of Boston](https://data.cityofboston.gov/) using the Socrata Open Data API endpoint *.json* URL. The last two data sets were scraped from Google using the Google API. 


The five new data sets we have created as a result of these 7 data sets are (informally) called:

- Boston Grid Cell GPS Centers (1000-FT Cells)
- Community Indicators Location and Score
- Boston Grid Cells Inverse Community Score
- Distinct Entertainment Licenses (without restaurants)
- Boston Grid Cells Crime Incidence 2012 - 2015

The interesting question we are trying to solve is this: if, given different "types" of city establishments/infrastructure that can either be categorized as 
indicators of "stronger community" or "weaker community", can we find other correlations with other indicators of inequality or "instability" in Boston and 
address common problems in terms of this social aspect? Stated in another, more concrete way -- if we were to add a new "community hotspot" or take away an
existing "tourist establishment" in order to maximize the benefits to Bostonians across a variety of metrics -- economic, social, infrastructural, etc. --
where would we do so?


These data sets were combined as a preliminary test to this question, using crime statistics, and seeing if there was a correlation in the 1000x1000-foot 
cells with crime incidence and what we scored as a "community value". The public fishing locations, community supported
agriculture pickups, and library 
locations were grouped and taken to be "community indicators", while entertainment, active food establishment license, and parking lot locations were taken 
to be "anti-community indicators" -- with the rationale that entertainment is a form of escapism from where one currently is (with large parking lots/garages 
designated as private, doing no public good), and that food establishments similarly exist to give people a break from eating around their community (and often 
being tourist spots as well). 

For project 2, we decided to create a well-defined heatmap of Boston (preliminarily, we simply have the values necessary to create the heatmap visualization 
across a 2D map of Boston), where each geographic 1000x1000-ft cell in a grid of cells across Boston has a computed indicator of how "strong" or "weak" community 
at that location is.

- Scour different data-sets as to how they might indicate "pro" or "anti" community indicators; the key here is to find fixed addresses/buildings/locations well-known for a specific purpose that are static over time/space (i.e., fixed points in the map of Boston)
- Once we've processed/grouped the data-sets accordingly to unique location points, we then determine whether a single location is pro or anti-community
- Once we have a dataset of all "pro" or "anti-community" locations/points in the map of Boston, we then figure out how to scale, appropriately, the pro vs. anti-community datasets against each other (since it's anticipated that there are plentifully more anti-community points than pro-community)
	- For each point that we deemed to be "pro-community", we marked the community indicator with a positive 1. 
	- For each point that we deemed to be "anti-community", we marked the community indicator with a negative 1.
	- Moreover, we kept a running sum of the total "pro-community" and "anti-community", and then we created a ratio from that. Specifically, we got the ratio 198/1858, where 198 is the count of "pro-community" points and 1858 is the count of "anti-community" points. From there, we multiply all the negative community indicators by ratio so that the negative indicators aren't overpowering the positive indicators (as there are less of the latter than the former).
- Once we've figured out our precise scale of weighting anti-community vs pro-community locations/points across Boston, we then produce our heatmap of "how much community" each point in the map of Boston has


## Algorithms, Tools, and Methods

The following data sets were retrieved, stored, and transformed in some specific way, all in order to facilitate the
creation of 2D-Sphere indexing in MongoDB (and for reference, 2D-Sphere indexing in MongoDB is a useful tool in comparing formatted geolocation data en-masse 
in MongoDB's databases (see [this](https://docs.mongodb.com/manual/core/2dsphere/ for reference))):
- Public Access Fishing Locations: 
	- The existing 'location' field was renamed to 'location_addess'
	- The existing 'map_location' field, in correct GeoJSON format to create the 2D-Sphere indexes, was renamed to 'location'
- Community Supported Agriculture Pickups:
	- Ditto from Public Access Fishing Locations
- Active Food Establishment Licenses:
	- The 'location' field was already in correct GeoJSON format, so the 2D-Sphere index was simply created for it in MongoDB
- Entertainment Licenses:
	- The 'location' field was, as a string, in '(latitude, longitude)' format -- so it was parsed and turned into a correct GeoJSON object
		(which, for reference, is in {'type': 'Point', 'coordinates': [longitude,latitude]} format); otherwise, that data-entry was deleted,
		as had to occur for a few rows that had malformed data
	- For project two, we further streamlined this dataset by:
		- Eliminating duplicate licenses within the dataset and
		- Eliminating any overlap between Active Food Establishment Licenses and Entertainment Licenses. Since there were some inconsistencies in regards to how each entry's location (latitude, longitude, city, street number, etc.) was formatted, we could only approximately find this overlap. 
- Issued Moving Truck Permits:
	- The existing 'location' field was renamed to 'location_details'
	- The 'location' field was added and, from the existing (now) 'location_details' field, we parsed their location_details.latitude and
		location_details.longitude sub-fields (both strings) and added the geolocation data in correct GeoJSON format
	- For project two, we decided to not include this dataset because there was no way of determing where the truck was coming from and going to.
- Year Round Swimming Pools
	- We transformed the included coordinate system in a similar way to how we transformed the Issued Moving Truck Permits. However, since the coordinates were in a format that we didn't understand, we manually input each address into GeoPy to find the latitude and longitude for each pool.
	- We decided to not transform this dataset any further for project two, because we still need to find a way to distinguish between the public pools (which are an indicator of positive community) and the pools that require an admission fee (which we would categorize an a negative community indicator).
- Libraries:
	- Scraped GPS locations using GoogleMaps API and other resources listing libraries in Boston
- Parking Lots/Garages:
	- Scraped GPS locations using GoogleMaps API and other resources listing parking lots/garages in Boston
	
	
## Dependencies

Please make sure to install the following using pip:

```
pip3 install pyshp
```

```
pip3 install pyproj
```

```
pip3 install -U googlemaps
```

Moreover, make sure you run the files in this order:

1. `retrieveData.py`
2. `libraries.py`
3. `parking.py`
4. `cleanup.py`
5. `combineRestaurantEnt.py`
6. `scoreLocations.py`
7. `gridCenters.py`
8. `distances.py`
9. `crimeRates.py`


The transformations/algorithms used to create the five new data sets occurred in the following manner.

1. Boston Grid Cell GPS Centers (1000-FT Cells)

	* Taking the Boston Shapefile (downloaded from this resource: http://www.arcgis.com/home/item.html?id=734463787ac44a648fe9119af4e98cae) and its coordinate points (while also finding the necessary coordinate-system reprojection into standard GPS coordinates), we found/used only the mainland and airport/East Boston geographic divisions of the shapefile, and subsequently divided up Boston into a grid of roughly ~1000x~1000-ft cells, and output into the "Boston Grid Cell GPS Centers (1000-FT Cells)" dataset the list of about ~1700 coordinates ((longitude, latitude) as (x,y) coordinates), each coordinate the respective center of a cell/box in the Boston-divided grid.
	
2. Community Indicators Location and Score

	* First, we took seven datasets:
	
		- Crime Incident Reports (from July 2012 - August 2015)
		- Public Access Fishing Locations, Issued Moving Truck Permits
		- Active Food Establishment Licenses, Entertainment Licenses
		- Community Supported Agriculture Pickups
		- Year-Round Swimming Pools.
		- Boston Parking Lots
		- Boston Libaries
		
	* We assumed that Fishing Locations, CSA pickups, year round pools and libraries have positive effect on a community. That is why we assigned community_score = 1 for each location. (for now, we ignored the pools dataset; will incorporate in Project 3)
	* Analogously, for each location that fell under the category of "anti-community" (such as Restaurant Licenses, Parking, and Entertainment Licenses), we assigned a community_score = -1.
	* Then, we multiply each "anti-community" community_score by the ratio that was detailed above.

3. Boston Grid Cells Inverse Community Score

	* For each GPS center, we calculated the distance between itself and all the location points using the distance formula.
	* Then, we multpled each respective calculated distance by the community score (from 'Community Indicators Location and Score') to obtain the overall impact on the cell's GPS center.
	* Then, we took the inverse of the entire sum and took that to be that specific cell's "Community Score/Value".

4. Distinct Entertainment Licenses (without restaurants)

	* First, we cleaned up the Entertainment Licenses dataset to exclude multiple entries for one license.
	* Then, we looked for the overlap between the Restaurant Licenses and the Entertainment Licenses by comparing the latitude, longitude, street name, street number, city, etc.
	* If it was the case that the same entry was found in both datasets, remove the entry from the Entertainment Licenses dataset, as that means that specific entry is actually a restaurant.

5. Boston Grid Cells Crime Incidence 2012 - 2015

	* We looked at the crime data and the output from Boston Grid Cell GPS Centers(1000-FT Cells). From here, we keep a running count of how many crimes occured within each 1000x1000ft cell, as determined by finding the geographically closest cell's GPS center for this current crime's reported location.
