Bus Route Optimization
=======================

Contributors: Adrian Law (alaw), Mark Bestavros (markbest),  Tyrone Hou (tyroneh)

###Goals for the Project
Optimize access and coverage to public transportation by changing bus stops along routes  
Determine a stable bus schedule given information about traffic (Google maps API/NextBus) & determine optimum bus allocation for maximum efficency and performance 
  
###To Run the project

####Installing Dependencies
* dml
* prov
* xmltodict
* pyshp
* pyproj
* dbfread
  
\* **MAKE SURE YOU HAVE MONGOD RUNNING FIRST AND AUTH'D** \*
  
getData.py: Pulls raw data and stores them inside MongoDB  
transformData.py: Retrieves raw data and reformats/transforms them for later use  
mapData.py: Simple matplotlib scatter plot of current points in datset for visualization purposes (Please replace with D3)
  
getData.py and transformData.py has execute() and provenance() methods, which are called and stored properly by the run() method. __main__ automatically executes the run() method. data2Geo.js contains the map-reduce functions for MongoDB, which are called by the execute() in transformData.py.
  
First run getData.py and then transformData.py to properly retrieve and transform the datasets.
  
###Bus Data Sources 
  
For current bus data
*	Bus Routes and Stations (MassGIS) 
*	Bus Route Mileage (MassGIS) 
*	Real time next-bus api (NextBus API scraping) 
	*	Mbta bus location data (for a few weeks)
  
For modeling 
*	T-stops (MBTA API) 
	*	T-stops Ridership (MBTA BlueBook) **-- Optional --**
*	Commuter rail stops (MBTA API)
	*	Commuter rail Ridership (MBTA BlueBook) **-- Optional --**
*	Hubway locations (Hubway Data Challenge)
*	Residential properties (Boston, Cambridge, Somerville, Brookline Data Portal)
*	2010 Population Census of relevant cities (MassGIS)
  
###Data Transformations
  
- [x] Standardize all property data to the GeoJSON format
	- [x] convert Brookline polygon coordinates to point data
	- [x] include type of property (residential or commercial)
	- [x] include # of bedrooms (max(numbedrooms,1) or max(# stories * 2, 1) for Brookline
- [x] Estimate population per property using Census data and averaging people per living unit
- [x] Reformat T and Commuter Rail routes to list of stops with ownerships of routes using the GeoJSON format
	- [ ] include capacities/estimates of ridership **-- Optional --**
- [x] Combine T-stops, Commuter Rail Stops, and Hubway station locations as points for "connections" using the GeoJSON format
- [x] Standardize Bus routes & Bus stops using GeoJSON polyline and points formats
 	- [ ] Using Bus coordinate data & distance estimates (using coordinate distance/Route completion difference/Google Maps API), calcuate average speed + deviation and record corresponding average completion time of a route + deviation **-- To Do --**
- [ ] \*Note: Drop datapoints outside of current purview **-- To Do --**
  
###Optimization Algorithms  
  
1. **Bus station optimization**:

2. **Bus allocation optimization**: The second most important consideration in optimizing bus routes is the number of buses each route should be allocated. Using MBTA bus location data with estimates of their average speed and deviation, the average completion time of a route per bus plus the deviation of that time can be derived. Two metrics used to measure the allocation is the latency of the route (on average how long it takes for a stop to be serviced) and the inefficiency of the allocation (the probability that two bus schedules will overlap each other at some point).  Assuming that the distribution of completion time is normally distributed, and that buses will be sent out at equal intervals to maximize coverage, the formula for optimization is below:  
  
> Score for Allocation = \(\(Average Time / Buses\) \* Stops\) \+ \(\(Area of Intersection between N\(0,Deviation\) and N\(Average Time / Buses\)\) \* Buses \)

Total latency is simplified in that inter-stop distance is not calculated, rather latency is the average interarrival time (completion time / k) multiplied by n, where n is the number of stops and k is the number of buses. Inefficiency can be measured by the total area of intersection of k normal distributions multiplied by the number of buses.  
To compute optimization, run optimizeBusAllocation


  





