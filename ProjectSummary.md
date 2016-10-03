#Goals for the Project
Optimize access and coverage to public transportation by changing bus stops and routes
Determine a stable bus schedule given information about traffic (Google maps API/NextBus)
how can we coordinate with bus and T stops/commuter line/hubway by linking different systems together 



#Bus Data Sources 

For current bus data
*	Bus stops (MBTA API)
*	Bus routes (MBTA API)
*	Number of buses + mbta ridership data (MBTA Bluebook)
*	Real time next-bus api (NextBus/MBTA API scraping / google drive) 
	*	Mbta bus location data (for a few weeks)

For modeling 
*	T-stops (MBTA API)
*	Commuter rail stops (MBTA API)
*	Hubway locations (hubway)
*	Residential properties (Boston, Cambridge, Somerville, Brookline)

#Transformations

1) Standardize all property data to the GeoJSON format
	1.5) convert Brookline polygon coordinates to point data
2) Combine T-stops, Commuter Rail Stops, and Hubway station locations as points for "connections" (GeoJSON)
3) Define bus routes & bus stops with corresponding average time between each bus stop, # of buses and ridership