Bus Route Optimization
=======================

Contributors: Adrian Law (alaw), Tyrone Hou (tyroneh)

##Goals for the Project
Optimize access and coverage to public transportation by changing bus stops and routes
Determine a stable bus schedule given information about traffic (Google maps API/NextBus) & determine currenty efficency in coverage and transferability 
How can we coordinate with bus and T stops/commuter line/hubway by replacing current bus routes with generated ones

##To Run the project

getData.py and transformData.py has execute() and provenance() methods, which are called and stored properly by the run() method. __main__ automatically executes the run() method. data2Geo.js contains the map-reduce functions for MongoDB, which are called by the execute() in transformData.py. First run getData.py and then transformData.py to properly retrieve and transform the datasets.

##Bus Data Sources 

For current bus data
*	Bus stops (MBTA API)  -- Currently Scraping --
*	Bus routes (MBTA API) -- Currently Scraping --
*	Number of buses + mbta ridership data (MBTA Bluebook) TBD
*	Real time next-bus api (NextBus/MBTA API scraping / google drive) -- Currently Scraping --
	*	Mbta bus location data (for a few weeks)

For modeling 
*	T-stops (MBTA API) 
*	Commuter rail stops (MBTA API)
*	Hubway locations (Hubway Data Challenge)
*	Residential properties (Boston, Cambridge, Somerville, Brookline Data Portal)

##Data Transformations

1) Standardize all property data to the GeoJSON format
	1.5) convert Brookline polygon coordinates to point data
2) Reformat T and Commuter Rail routes to list of stops with ownerships of routes using the GeoJSON format
3) Combine T-stops, Commuter Rail Stops, and Hubway station locations as points for "connections" using the GeoJSON format
4) Define bus routes & bus stops with corresponding average time between each bus stop, # of buses and ridership (TO DO)