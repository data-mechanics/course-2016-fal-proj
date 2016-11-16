# course-2016-fal-proj (aliyevaa, bsowens, dwangus, jgtsui)
Project repository for the course project in the Fall 2016 iteration of the Data Mechanics course at Boston University.

## Justification

The data sets we have chosen are:

- Crime Incident Reports (from July 2012 - August 2015)
- Public Access Fishing Locations, Issued Moving Truck Permits
- Active Food Establishment Licenses, Entertainment Licenses
- Community Supported Agriculture Pickups
- Year-Round Swimming Pools.
- Boston Parking Lots
- Boston Libaries

The first five datasets are pulled from [City of Boston](https://data.cityofboston.gov/) using the Socrata Open Data API endpoint *.json* URL. The last two data sets were scraped from Google using the Google API. 


The five new data sets we have created as a result of these 7 data sets are (informally) called:

- Boston Grid Cell GPS Centers (1000-FT Cells)
- Community Indicators Location and Score
- Boston Grid Cells Inverse Community Score
- Distinct Entertainment Licenses (without restaurants)
- Boston Grid Cells Crime Incidence 2012 - 2015.

The interesting question we are trying to solve is this: if, given different "types" of city establishments/infrastructure that can either be categorized as 
indicators of "stronger community" or "weaker community", can we find other correlations with other indicators of inequality or "instability" in Boston and 
address common problems in terms of this social aspect? Stated in another, more concrete way -- if we were to add a new "community hotspot" or take away an
existing "tourist establishment" in order to maximize the benefits to Bostonians across a variety of metrics -- economic, social, infrastructural, etc. --
where would we do so?


These data sets were combined as a preliminary test to this question, using crime statistics, and seeing if there was a correlation in the location where a
crime occurred and the frequency of "anti-community" and "community" indicators within a 1-mile radius. The public fishing locations and community supported
agriculture pickups were grouped and taken to be "community indicators", while entertainment and active food establishment licenses were taken to be "anti-
community indicators" -- with the rationale that entertainment is a form of escapism from where one currently is, and that food establishments similarly 
exist to give people a break from eating around their community (and often being tourist spots as well). The moving truck permits data set was offered as a 
frame of reference to the proposed groupings of community and anti-community indicators -- as it's unclear whether all moving truck permits issued are for
people leaving or entering Boston. 

For project 2, we decided to create a well-defined heatmap of Boston, where each geographic location has an indicator of how "strong" or "weak" community at that location is.

- Scour different data-sets as to how they might indicate "pro" or "anti" community indicators; the key, here, is to find fixed addresses/buildings/locations well-known for a specific purpose that are static over time/space (i.e., fixed points in the map of Boston)
- Once we've processed/grouped the data-sets accordingly to unique location points, we then determine whether a single location is pro or anti-community
- Once we have a dataset of all "pro" or "anti-community" locations/points in the map of Boston, we then figure out how to scale, appropriately, the pro vs. anti-community datasets against each other (since it's anticipated that there are plentifully more anti-community points than pro-community)
- (the key here is to be as isolated/objective as possible from whatever our perceived ultimate effects we're cross-referencing -- i.e., we shouldn't be changing how we scale or changing our definition of "community" after we know, for example, the crime heatmap or that an area has a high incidence of crime)
- Once we've figured out our precise scale of weighting anti-community vs pro-community locations/points across Boston, we then produce our heatmap of "how much community" each point in the map of Boston has
- (another potentially interesting aspect would be to add the dimension of time for our heatmaps -- i.e., see trends of how our definition of community has changed the heatmap over time)
- (another implementation note -- weighting individual "pro-community" locations/points against each other -- i.e. determining how "strong" a particular community-location is, relative to all others... maybe based somehow on frequency of traffic... or web traffic... or surrounding population density, etc.)
- We should take care to not include data sets of fixed locations (at least with regards to pro-community) that are heavily regulated by the government, as the government might have different incentives for putting ____ (like bus stops or building a new public school, etc.) at location (x,y), to minimize/maximize other economic/transportation/population metrics, independent of our study of how community affects certain metrics and how those same metrics affect community back vicer-versa (in other words, if a correlation exists)

## Algorithms, Tools, and Methods

The following data sets were retrieved, stored, and transformed in some specific way, all in order to facilitate the
creation of 2D-Sphere indexing in MongoDB (and for reference, 2D-Sphere indexing in MongoDB is a useful tool in comparing formatted geolocation data en-masse 
in MongoDB's databases (see https://docs.mongodb.com/manual/core/2dsphere/ for reference)):
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
	- We decided to not transform this dataset any further for project two, because we still need to find a way to distinguish between the public pools (which are an indicator of positive community) and the pools that require an admission fee (whihch we would categorize an a negative community indicator).
		
		
**FIX DIS TOO**		
The transformations/algorithms used to create the three new data sets occurred in the following manner, applied in a near-identical manner for each:
- Copy the existing crime data set (whose geolocation data was already correctly formatted) as the new data set to be created, that will be soon edited in-place
- For each crime/entry in this new data set, update as follows:
	- Create a new field called '<>_indicators_1600m_radius', with value = 
		- Find the size of the filtered set of all entries in the corresponding data sets based on category (community, anti-community, moving permits), based on 
			whether their location exists within 1600 meters of the given crime's location
