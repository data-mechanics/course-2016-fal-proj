# Finding Community Hotspots in Boston -- Report
Project repository for the course project in the Fall 2016 iteration of the Data Mechanics course at Boston University.

## Group Members

Assel Aliyeva, Benjamin Owens, David Wang, and Jennifer Tsui

## Introduction

The interesting question we are trying to solve is this: if, given different "types" of city establishments/infrastructure that can either be categorized as 
indicators of "stronger community" or "weaker community", can we find other correlations with other indicators of inequality or "instability" in Boston and 
address common problems in terms of this social aspect? Stated in another, more concrete way -- if we were to add a new "community hotspot" or take away an
existing "tourist establishment" in order to maximize the benefits to Bostonians across a variety of metrics -- economic, social, infrastructural, etc. --
where would we do so?


The data sets we have chosen are:

- [Crime Incident Reports (from July 2012 - August 2015)](https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx/data)
- [Property Assessment 2016](https://data.cityofboston.gov/Permitting/Property-Assessment-2016/i7w8-ure5/data)
- [Public Access Fishing Locations](https://data.cityofboston.gov/dataset/Public-Access-Fishing-Locations/9tfg-3jic/data)
- [Active Food Establishment Licenses](https://data.cityofboston.gov/Permitting/Active-Food-Establishment-Licenses/gb6y-34cq/data)
- [Entertainment Licenses](https://data.cityofboston.gov/Permitting/Entertainment-Licenses/qq8y-k3gp/data)
- [Community Supported Agriculture (CSA) Pickups](https://data.cityofboston.gov/dataset/Community-Supported-Agriculture-CSA-Pickups/rvw3-dget/data)
- [Year-Round Swimming Pools](https://data.cityofboston.gov/Public-Property/Year-Round-Swimming-Pools/rtqb-8pht/data) (Not used for final result)
- [Issued Moving Truck Permits](https://data.cityofboston.gov/Permitting/Issued-Moving-Truck-Permits/bzif-fkwd) (Not used for final result)
- Boston Parking Lots
- Boston Libaries

The first eight datasets are pulled from [City of Boston](https://data.cityofboston.gov/) using the Socrata Open Data API endpoint *.json* URL. The last two data sets were scraped from Google using the Google API. 

The six new data sets we have created as a result of these 7 data sets are called:

- created in `combineRestaurantEnt.py`
	- Distinct Entertainment Licenses (without restaurants)
	
- created in `gridCenters.py` 
	- Boston Grid Cell GPS Centers (1000-FT Cells)
	- *Note:* Only David can run this file, since he used a deprecated package that we don't have access to. As a result, we kept track of the cell centers by referring to the file titles `centers.txt`
	
- created in `scoreLocations.py`
	- Community Indicators Location and Score
	
- created in `distancesCommunityScoreCalculations.py`
	- Boston Grid Cells Inverse Community Score
	
- created in `crimeRates_and_propertyVals_Faster_Aggregation.py`
	- Boston Grid Cells Crime Incidence 2012 - 2015
	- Property Assessment 2016

These data sets were combined as an answer to the question above. We wanted to see how crime or property value were correlated in the 1000x1000-foot 
cells with what we scored as a "community value". The public fishing locations, community supported
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
	- Moreover, we kept a running sum of the total "pro-community" and "anti-community", and then we created a ratio from that. From there, we multiply the larger set of points by this ratio to make sure that it doesn't overpower the smaller set. 
- Once we've figured out our precise scale of weighting anti-community vs pro-community locations/points across Boston, we then produce our heatmap of "how much community" each point in the map of Boston has

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
	- For project two, we decided to not include this dataset because there was no way of determing where the truck was coming from and going to. We didn't include this in the final submission either, as there wasn't enough time to integrate it. 
- Year Round Swimming Pools
	- We transformed the included coordinate system in a similar way to how we transformed the Issued Moving Truck Permits. However, since the coordinates were in a format that we didn't understand, we manually input each address into GeoPy to find the latitude and longitude for each pool.
	- We decided to not transform this dataset any further for project two, because we still need to find a way to distinguish between the public pools (which are an indicator of positive community) and the pools that require an admission fee (which we would categorize an a negative community indicator).
- Libraries:
	- Scraped GPS locations using GoogleMaps API and other resources listing libraries in Boston
- Parking Lots/Garages:
	- Scraped GPS locations using GoogleMaps API and other resources listing parking lots/garages in Boston
	
### Project 2 and 3 algorithms

The transformations/algorithms used to create the five new data sets occurred in the following manner.

1. Community Indicators Location and Score

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

2. Boston Grid Cell GPS Centers (1000-FT Cells)

	* Taking the Boston Shapefile (downloaded from this resource: http://www.arcgis.com/home/item.html?id=734463787ac44a648fe9119af4e98cae) and its coordinate points (while also finding the necessary coordinate-system reprojection into standard GPS coordinates), we found/used only the mainland and airport/East Boston geographic divisions of the shapefile, and subsequently divided up Boston into a grid of roughly ~1000x~1000-ft cells, and output into the "Boston Grid Cell GPS Centers (1000-FT Cells)" dataset the list of about ~1700 coordinates ((longitude, latitude) as (x,y) coordinates), each coordinate the respective center of a cell/box in the Boston-divided grid.
	* Note: this may not run for you. We've included a file called `centers.txt` which contains the output equivalent to what would be stored in the database. The code where we stored to the database is still included, but our code depends on `centers.txt` for the sake of being flexible.
	

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

6. Property Assessment 2016
	
	* We looked at the property value data and the output from the Boston Grid GPS Centers(1000-FT Cells). From here, we take the log of the average property value in a given cell.

## Dependencies

Please make sure to install the following using pip:

We have committed setup scripts for a MongoDB database that will set up the database and collection management functions that ensure users sharing the project data repository can read everyone's collections but can only write to their own collections. Once you have installed your MongoDB instance, you can prepare it by first starting `mongod` _without authentication_:
```
mongod --dbpath "<your_db_path>"
```
If you're setting up after previously running `setup.js`, you may want to reset (i.e., delete) the repository as follows.
```
mongo reset.js
```
Next, make sure your user directories (e.g., `alice_bob` if Alice and Bob are working together on a team) are present in the same location as the `setup.js` script, open a separate terminal window, and run the script:
```
mongo setup.js
```
Your MongoDB instance should now be ready. Stop `mongod` and restart it, enabling authentication with the `--auth` option:
```
mongod --auth --dbpath "<your_db_path>"
```


Then, run the following file. The command line prompt would look like this:

```
python3 MAIN_SCRIPT.py
```

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
	- For project two, we decided to not include this dataset because there was no way of determing where the truck was coming from and going to. We didn't include this in the final submission either, as there wasn't enough time to integrate it. 
- Year Round Swimming Pools
	- We transformed the included coordinate system in a similar way to how we transformed the Issued Moving Truck Permits. However, since the coordinates were in a format that we didn't understand, we manually input each address into GeoPy to find the latitude and longitude for each pool.
	- We decided to not transform this dataset any further for project two, because we still need to find a way to distinguish between the public pools (which are an indicator of positive community) and the pools that require an admission fee (which we would categorize an a negative community indicator).
- Libraries:
	- Scraped GPS locations using GoogleMaps API and other resources listing libraries in Boston
- Parking Lots/Garages:
	- Scraped GPS locations using GoogleMaps API and other resources listing parking lots/garages in Boston
	
### Project 2 and 3 algorithms

The transformations/algorithms used to create the five new data sets occurred in the following manner.

1. Community Indicators Location and Score

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

2. Boston Grid Cell GPS Centers (1000-FT Cells)

	* Taking the Boston Shapefile (downloaded from this resource: http://www.arcgis.com/home/item.html?id=734463787ac44a648fe9119af4e98cae) and its coordinate points (while also finding the necessary coordinate-system reprojection into standard GPS coordinates), we found/used only the mainland and airport/East Boston geographic divisions of the shapefile, and subsequently divided up Boston into a grid of roughly ~1000x~1000-ft cells, and output into the "Boston Grid Cell GPS Centers (1000-FT Cells)" dataset the list of about ~1700 coordinates ((longitude, latitude) as (x,y) coordinates), each coordinate the respective center of a cell/box in the Boston-divided grid.
	* Note: this may not run for you. We've included a file called `centers.txt` which contains the output equivalent to what would be stored in the database. The code where we stored to the database is still included, but our code depends on `centers.txt` for the sake of being flexible.
	

3. Boston Grid Cells Inverse Community Score

With authentication enabled, you can start `mongo` on the repository (called `repo` by default) with your user credentials:
```
mongo repo -u alice_bob -p alice_bob --authenticationDatabase "repo"
```
However, you should be unable to create new collections using `db.createCollection()` in the default `repo` database created for this project:
```
> db.createCollection("EXAMPLE");
{
  "ok" : 0,
  "errmsg" : "not authorized on repo to execute command { create: \"EXAMPLE\" }",
  "code" : 13
}
```
Instead, load the server-side functions so that you can use the customized `createTemp()` or `createPerm()` functions, which will create collections that can be read by everyone but written only by you:
```
> db.loadServerScripts();
> var EXAMPLE = createPerm("EXAMPLE");
```
Notice that this function also prefixes the user name to the name of the collection (unless the prefix is already present in the name supplied to the function).
```
> EXAMPLE
alice_bob.EXAMPLE
> db.alice_bob.EXAMPLE.insert({value:123})
WriteResult({ "nInserted" : 1 })
> db.alice_bob.EXAMPLE.find()
{ "_id" : ObjectId("56b7adef3503ebd45080bd87"), "value" : 123 }
```
For temporary collections that are only necessary during intermediate steps of of a computation, use `createTemp()`; for permanent collections that represent data that is imported or derived, use `createPerm()`.

If you do not want to run `db.loadServerScripts()` every time you open a new terminal, you can use a `.mongorc.js` file in your home directory to store any commands or calls you want issued whenever you run `mongo`.

## Other required libraries and tools

You will need the latest versions of the PROV and DML Python libraries. If you have `pip` installed, the following should install the latest versions automatically:
```
pip install prov --upgrade --no-cache-dir
pip install dml --upgrade --no-cache-dir
```
If you are having trouble with `lxml`, you could try retrieving it [here](http://www.lfd.uci.edu/~gohlke/pythonlibs/).

## Formatting the `auth.json` file

6. Property Assessment 2016
	
	* We looked at the property value data and the output from the Boston Grid GPS Centers(1000-FT Cells). From here, we take the log of the average property value in a given cell.


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

Then, run the following file. The command line prompt would look like this:

```
python3 MAIN_SCRIPT.py
```
## Visualizations

Fig. 1: Community Heat Map — We take 1000x1000-ft cells in a grid of cells across Boston, and compute the indicator of how "strong" or "weak" community at that location is. The higher community areas have a redder coloring. 

![alt tag](http://i68.tinypic.com/2d6w36o.png) 

Fig. 2: Crime Heat Map — We looked at the crime data and the output from Boston Grid Cell GPS Centers(1000-FT Cells). We then kept a count of how many crimes occured in that area. 

![alt tag](http://i67.tinypic.com/19roth.png)


Fig. 3: Correlations — This bar chart is made from a computation of the correlation coefficient, based on the community score vs. crime and property values. This was calculated with the following methods, which were taken from the [notes](http://cs-people.bu.edu/lapets/591/s.php). This code can be found in `correlation.py`

```
def avg(x):
    return sum(x)/len(x)

def stddev(x):
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): 
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): 
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))
```

![alt tag](http://i66.tinypic.com/351g5y8.png)

Fig. 4-8: The following plots all the community scores, crime scores, and property scores in an interactive scatter plot. the community scores are blue, the crime scores are purple, and the property scores are green. This helps to better illustrate why we got the correlation coefficient values that we did -- you can see that the overlap of crime scores and property scores is very apparent. This may explain why the correlation coefficients of crime vs. community and property vs community are so close. 

![alt tag](https://puu.sh/sRaCw/5e6b8afbd0.png)

![alt tag](https://puu.sh/sRaEe/13b4a6fca6.png)

![alt tag](https://puu.sh/sRaFv/427a0c882f.png)

![alt tag](https://puu.sh/sRaGn/0b934a22e3.png)

![alt tag](https://puu.sh/sRaGR/6f6104b88a.png)

*Note:* All of this information was generated by the file `correlation.py`. The heatmaps were built upon a series of `.txt` files, while the scatterplot was created from a single `.csv` filed called `CommunityPropertyCrimeScatter.csv`

## Conclusion

Given the correlations, we found that there was a fairly negative correlation between the community score with both crime and property values, respectively. Property values’ correlation coefficient with community (as we defined it) appeared to be more negative than community’s correlation coefficient with crime, which means that property value is more negatively correlated with community. 

To extend on this, we may choose to extend on the idea of these correlations with community further. This would be interesting because the correlation coefficient may be a result, or even a victim of the ways that we obtained certain data sets. More specifically, the way that we created the community dataset may have introduced significant bias that we would have to overcome. To remedy this, we may find a point of comparison by calculating the p-value and considering every permutation of the data.

Furthermore, we could fine turn the metric of getting the crime score by taking each GPS center and calculating the distance between itself and all the crime location points using the distance formula. Then, we could multiply each respective calculated distance by the community score (from 'Community Indicators Location and Score') to obtain the overall impact on the cell's GPS center. Then, take the inverse of the entire sum and took that to be that specific cell's “Crime Score”. We tried to do this using a numpy matrix, but it proved to be too computationally intensive. 

Another way to extend on this project would be to take into account other datasets (such as Issued Moving Truck Permits) and finding how those would be correlated to community. 
