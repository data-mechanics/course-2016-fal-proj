# course-2016-fal-proj (aliyevaa, bsowens, dwangus, jgtsui)
Project repository for the course project in the Fall 2016 iteration of the Data Mechanics course at Boston University.

## Justification

The data sets I've chosen are the Crime Incident Reports (from July 2012 - August 2015), Public Access Fishing Locations, Issued Moving Truck Permits, 
Active Food Establishment Licenses, Entertainment Licenses, Community Supported Agriculture Pickups, and Year-Round Swimming Pools. The three new data sets
I've created as a result of these 7 data sets (all of them pulled from https://data.cityofboston.gov/ using the Socrata Open Data API endpoint .json URL)
are (informally) called Crime Instances v. Community Indicators, Crime Instances v. Anti-Community Indicators, and Crime Instances v. Moving Truck Permits
(all instances of crime are looked at within a one-mile, or 1600-meter radius of the indicators mentioned in the title).

The interesting question I'm trying to solve is this: if, given different "types" of city establishments/infrastructure that can either be categorized as 
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

The year-round swimming pools data set was excluded from the methods used for now (they would be community indicators), as I could not figure out what GPS/
location coordinate formats were being used in the data set.

## Algorithms, Tools, and Methods

The following data sets were retrieved from https://data.cityofboston.gov/ and store and transformed in some specific way, all in order to facilitate the
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
- Issued Moving Truck Permits:
	- The existing 'location' field was renamed to 'location_details'
	- The 'location' field was added and, from the existing (now) 'location_details' field, we parsed their location_details.latitude and
		location_details.longitude sub-fields (both strings) and added the geolocation data in correct GeoJSON format
		
The transformations/algorithms used to create the three new data sets occurred in the following manner, applied in a near-identical manner for each:
- Copy the existing crime data set (whose geolocation data was already correctly formatted) as the new data set to be created, that will be soon edited in-place
- For each crime/entry in this new data set, update as follows:
	- Create a new field called '<>_indicators_1600m_radius', with value = 
		- Find the size of the filtered set of all entries in the corresponding data sets based on category (community, anti-community, moving permits), based on 
			whether their location exists within 1600 meters of the given crime's location
