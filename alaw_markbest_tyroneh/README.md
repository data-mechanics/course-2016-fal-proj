Algorithmic Optimizations of Boston's Public Bus System
=======================

Contributors: Adrian Law (alaw), Mark Bestavros (markbest),  Tyrone Hou (tyroneh)

##Introduction and Motivation

Through various personal experiences as well as a plurality of opinion from the media and the general public, we know that Boston's public bus system is in dire need of improvement. Buses are frequently late, trips take a long time, and the system is generally unreliable and inefficient. We want to take steps to change that with this project by applying big-data techniques and algorithms to existing transit data (among other things) and developing approaches to optimize the efficiency and effectiveness of Boston's bus system.

##Goals for the Project
We have outlined two optimizations we wish to pursue with our project.
* Optimize access to and coverage of public transportation by moving existing stops along routes
* Determine a stable bus schedule given information about traffic (Google Maps API/NextBus) and determine optimum bus allocation for maximum efficiency and performance on all of Boston's bus routes
  
##Data Sources 
  
We looked at a significant amount of data sets for this project, most of which were used in at least one of the optimizations we ended up doing. 

For current bus data
*	Bus Routes and Stations (MassGIS) 
*	Bus Route Mileage (MassGIS) 
*	Real time next-bus API (NextBus API scraping) 
	*	Mbta bus location data (for a few weeks)
  
For modeling 
*	T-stops (MBTA API) 
	*	T-stops Ridership (MBTA BlueBook) **-- Optional --**
*	Commuter rail stops (MBTA API)
	*	Commuter rail Ridership (MBTA BlueBook) **-- Optional --**
*	Hubway locations (Hubway Data Challenge)
*	Residential properties (Boston, Cambridge, Somerville, Brookline Data Portal)
*	2010 Population Census of relevant cities (MassGIS)
  
##Data Transformations
  
- [x] Standardize all property data to the GeoJSON format
	- [x] convert Brookline polygon coordinates to point data
	- [x] include type of property (residential or commercial)
	- [x] include # of bedrooms (max(numbedrooms,1) or max(# stories * 2, 1) for Brookline
- [x] Estimate population per property using Census data and averaging people per living unit
- [x] Reformat T and Commuter Rail routes to list of stops with ownerships of routes using the GeoJSON format
	- [ ] include capacities/estimates of ridership **-- Optional --**
- [x] Combine T-stops, Commuter Rail Stops, and Hubway station locations as points for "connections" using the GeoJSON format
- [x] Standardize Bus routes & Bus stops using GeoJSON polyline and points formats
 	- [x] Using Bus coordinate data & distance estimates (using coordinate distance/Route completion difference/Google Maps API), calcuate average speed + deviation and record corresponding average completion time of a route + deviation
- [x] \*Note: Drop datapoints outside of current purview
  
##Optimization Algorithms  
  
1. **Bus station optimization**: The routes that run through metropolitan Boston distribute stops across the city. Running k-means can measure the efficiency of the current placement by comparing the means generated using the distribution of residential and non-residential properties and stations of other modes of public transportation in relation to these bus stops. This k-means algorithm differ from regular implementations in that every iteration has to project the new set of means back on to the closest segment of a bus route. The output for each route will show the original placement of stops with the calculated means.

2. **Bus allocation optimization**: The second most important consideration in optimizing bus routes is the number of buses each route should be allocated. Using MBTA bus location data with estimates of their average speed and deviation, the average completion time of a route per bus plus the deviation of that time can be derived. Two metrics used to measure the allocation is the latency of the route (on average how long it takes for a stop to be serviced) and the inefficiency of the allocation (the probability that two bus schedules will overlap each other at some point).  Assuming that the distribution of completion time is normally distributed, and that buses will be sent out at equal intervals to maximize coverage, the formula for optimization is below:  
  
![Allocation Score Formula](https://raw.githubusercontent.com/tyronehou/course-2016-fal-proj/master/alaw_markbest_tyroneh/poster/optimalAllocationFormula.gif)

Total latency is simplified in that inter-stop distance is not calculated, rather latency is the average interarrival time (completion time / k) multiplied by n, where n is the number of stops and k is the number of buses. Inefficiency can be measured by the total area of intersection of k normal distributions multiplied by the number of buses. Output in the collection OptimumAllocation stores the optimal number of buses for each route.


##Analysis of Optimizations

####Bus Allocation

![Optimal Allocation Graph](https://raw.githubusercontent.com/tyronehou/course-2016-fal-proj/master/alaw_markbest_tyroneh/poster/optimalAllocation.png)

The results we found from the optimization algorithm did consistently correlate with the results from actual allocations of each route; those routes that generally had more buses running on them due to length or importance were generally allocated more buses by our algorithm. This is most likely due to the fact that the importance, length and ridership of a route were captured by the number of stops on a route and the average and deviation of completion time per bus on that route. 

However, our algorithm also consistently under-allocates the number of buses per route across all routes. The total number of buses that the algorithm chose to allocate was 401, nearly half as many as the actual active number of buses running on MBTA’s boston routes (around 800). This suggests that our algorithm has shortfalls as a heuristic and should take into account more factors that we did not consider.



##To Run the Project

####Installing Dependencies
* dml
* prov
* xmltodict
* pyshp
* pyproj
* dbfread
* random
* rtree (requires libspatialindex; see below)
    * [libspatialindex] (https://libspatialindex.github.io/)
\* **MAKE SURE YOU HAVE MONGOD RUNNING FIRST AND AUTH'D** \*
  
**main.py**: executes all scripts in order and performs optimizations (-t: trial mode to do Algo R Sampling on datasets, -v: produce graphs for optimizations)  
**getData.py**: Pulls raw data and stores them inside MongoDB and records prov  
**transformData.py**: Retrieves raw data and reformats/transforms them for optimization use and records prov  
**mapData.py (TEMP)**: Simple matplotlib scatter plot of current points in datset for visualization purposes (Please replace with D3)
  
data2Geo.js contains the map-reduce functions for transforming the location based datasets to geoJSON format. Functions are called by the execute() in transformData.py.  
getAvgVels.js contains the map-reduce functions for calculates bus velocities based on nextBus data. Functions are called by the execute() in transformData.py
  
Run main.py to execute scripts in order, use flags to customize speed and output of algorithms


  





