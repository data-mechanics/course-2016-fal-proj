Algorithmic Optimizations of Boston's Public Bus System
=======================

Contributors: Adrian Law (alaw), Mark Bestavros (markbest),  Tyrone Hou (tyroneh)

##Introduction and Motivation

Through various personal experiences as well as a plurality of opinion from the media and the general public, we know that Boston's public bus system is in dire need of improvement. Buses are frequently late, trips take a long time, and the system is generally unreliable and inefficient. We want to take steps to change that with this project by applying big-data techniques and algorithms to existing transit data (among other things) and developing approaches to optimize the efficiency and effectiveness of Boston's bus system.

##Goals for the Project
We have outlined two optimizations we wish to pursue with our project:
* Optimize access to and coverage of public transportation by moving existing stops along routes
* Determine a stable bus schedule given information about traffic (Google Maps API/NextBus) and determine optimum bus allocation for maximum efficiency and performance on all of Boston's bus routes
  

  
##Optimization Algorithms  
  
1. **Bus station placement optimization**: The routes that run through metropolitan Boston distribute stops across the city according to a variety of considerations. Ideally stop placement would take into account geographic population density and reduce the distance and amount of time it takes for a person to reach the closest bus stop. We use the k-means clustering algorithm to generate means corresponding to residential properties in Boston, and use those means to derive new optimal bus stop locations along a route. To ensure that the means lie along the original bus route, we first project all residential areas within 0.5 km of a route to the closest point on the route. We then map these points into a one dimensional space so that the k-means will only move means along the route, then map the means back into two dimensional space to get the locations of the optimal bus stops.

2. **Bus allocation optimization**: The second most important consideration in optimizing bus routes is the number of buses each route should be allocated. Using MBTA bus location data with estimates of their average speed and deviation, the average completion time of a route per bus plus the deviation of that time can be derived. Two metrics used to measure the allocation is the latency of the route (on average how long it takes for a stop to be serviced) and the inefficiency of the allocation (the probability that two bus schedules will overlap each other at some point).  Assuming that the distribution of completion time is normally distributed, and that buses will be sent out at equal intervals to maximize coverage, the formula for optimization is below:  
  
![Allocation Score Formula](https://raw.githubusercontent.com/tyronehou/course-2016-fal-proj/master/alaw_markbest_tyroneh/poster/optimalAllocationFormula.gif)

Total latency is simplified in that inter-stop distance is not calculated; rather, latency is the average interarrival time (completion time / k) multiplied by n, where n is the number of stops and k is the number of buses. Inefficiency can be measured by the total area of intersection of k normal distributions multiplied by the number of buses. Output in the collection OptimumAllocation stores the optimal number of buses for each route.


##Analysis of Optimizations

####Stop Placement

![Sample Map of Route Coverage](https://raw.githubusercontent.com/tyronehou/course-2016-fal-proj/master/alaw_markbest_tyroneh/poster/mapSampleCoverage.png)

![K-Means Optimized Stops](https://raw.githubusercontent.com/tyronehou/course-2016-fal-proj/master/alaw_markbest_tyroneh/poster/KmeanStops.png)

We ran our algorithm on bus stops along Route 39: (Forest Hills Station - Back Bay Station via Huntington Ave.) The route provided us with a good distribution of residential points along the route, ensuring that the kmeans did not cluster points with too much bias. Our results showed that new stops moved along the route towards areas with more residential areas, which may indicate that the current stop placement is not optimal.

However, our algorithm did not take into account cases when the spread of residential areas near to a route was scarce. For example, many bus routes extending past the metropolitan Boston area had a very low distribution of residential points. This would bias our algorithm to shift most of the bus stops inwards, leaving the outer routes with no stops. Additionally, we want to minimize the amount of travel time for each rider, as well increase the connectivity of the bus system to other modes of public transit, e.g. the T and Hubway. Thus we would like to run our algorithm on weighted data points from commercial properties, T stop and Hubway station locations in Boston.

####Bus Allocation

![Optimal Allocation Graph](https://raw.githubusercontent.com/tyronehou/course-2016-fal-proj/master/alaw_markbest_tyroneh/poster/optimalAllocation.png)

The results we found from the optimization algorithm did consistently correlate with the results from actual allocations of each route; those routes that generally had more buses running on them due to length or importance were generally allocated more buses by our algorithm. This is most likely due to the fact that the importance, length and ridership of a route were captured by the number of stops on a route and the average and deviation of completion time per bus on that route. 

However, our algorithm also consistently under-allocates the number of buses per route across all routes. The total number of buses that the algorithm chose to allocate was 401, nearly half as many as the actual active number of buses running on MBTA’s Boston routes (around 800). This suggests that our algorithm has shortfalls as a heuristic and should take into account more factors that we did not consider.

##Conclusions and Future Work

Though the datasets we produced as part of our analysis are interesting and generally seem to fit the characteristics of the bus system, there are certainly a few things that can be improved upon in future work, as mentioned in the analysis sections above. Expanding our datasets to include commercial data in addition to residential data would go a long way towards improving our stop placement analysis analysis to more accurately reflect how many people actually use the public bus system: to travel to and from work. Additionally, many routes expand beyond the scope of our current datasets, so including more data in general beyond the core neighborhoods of Boston, Cambridge, Brookline, and Somerville would also be a prudent next step. Finally, further study of the factors behind bus stop allocation is certainly warranted, given the noted discrepancy between our analysis and the actual allocations.

Looking forward, it's natural to ask how these optimizations could be applied in the real world. Unfortunately, there are some practical barriers that make the application of this study difficult--specifically, politics. This is much more of an issue for the stop placement optimization: the [politics](https://en.wikipedia.org/wiki/NIMBY) and cost involved with moving one bus stop--let alone every single one in the city--is absurd.

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
* flask
* matplotlib
* numpy
  
  \* **MAKE SURE YOU HAVE MONGOD RUNNING FIRST AND AUTH'D** \*
  
**main.py**: executes all scripts in order and performs optimizations (-t: trial mode to do Algo R Sampling on datasets, -v: produce graphs for optimizations)  
**getData.py**: Pulls raw data and stores them inside MongoDB and records prov  
**transformData.py**: Retrieves raw data and reformats/transforms them for optimization use and records prov  
**mapData.py (TEMP)**: Simple matplotlib scatter plot of current points in datset for visualization purposes (Please replace with D3)
  
data2Geo.js contains the map-reduce functions for transforming the location based datasets to geoJSON format. Functions are called by the execute() in transformData.py.  
getAvgVels.js contains the map-reduce functions for calculates bus velocities based on nextBus data. Functions are called by the execute() in transformData.py
  
Run main.py to execute scripts in order, use flags to customize speed and output of algorithms
  
**Running Visualizations**: run the server.py file under /visualizations  
**127.0.0.1:5000/kmeans**: Bus Stop Optimization Visualization for Route 39
**127.0.0.1:5000/allocation**: Bus Allocation Optimization Visualization




