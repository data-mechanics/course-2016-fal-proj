#CS591 Project Proposal 

Our goal is to gather information on the sanitation-related trends of Boston neighborhoods. Essentially, we want to be able to analyze this data to propose possible solutions/alternatives to help reduce sanitary code violations, improve overall cleanliness, optimize trash collection schedules, and prevent trash overfill. The types of data we use in this project primarily deal with health code violations pertaining to excessive waste and presence of rodents. We also use data pertaining to the locations and collection statistics of Boston's "Big Belly" trash compactors. 

##Our Data Sets

Big Belly Alerts 2014: 
https://data.cityofboston.gov/resource/nybq-xu5r.json

Master Address List: 
https://data.cityofboston.gov/resource/je5q-tbjf.json

Code Enforcement - Building and Property Violations: 
https://data.cityofboston.gov/resource/w39n-pvs8.json

Food Establishments Inspections:
'https://data.cityofboston.gov/resource/427a-3cn5.json'

Mayors 24 Hour Hotline (Cases created last 90 days):
https://data.cityofboston.gov/resource/jbcd-dknd.json


##Running Scripts

1. Run `gatherDataSets.py` in order to retrieve and store all required raw data

2. Run the other transformation scripts (order doesn't matter):
    
    `bigBelly.py`:
    Uses "Big Belly Alerts 2014" dataset and performs transformations to find the amount of big belly compactors and the overall average fullness of Boston geolocations.

    How to run:
    ```
    $ python3 bigBelly.py
    ```

    `codeViolations.py`:
    Uses "Code Enforcement" and "Food Establishments Inspections" datasets and performs transformations to find health/sanitation related violations and the number of times they occured within each Boston zipcode.

    How to run:
    ```
    $ python3 codeViolations.py
    ```

    `serviceRequests.py`:
    Uses "Mayors 24 Hour Hotline" dataset and performs transformations to find the types of santiation-related requests and the number of times it was request for each Boston zipcode.

    How to run:
    ```
    $ python3 serviceRequests.py
    ```

    `trashSchedules.py`:
    Uses "Master Address List" dataset and performs transformations to find the trash collection days for each Boston zipcode. 

    How to run:
    ```
    $ python3 trashSchedules.py
    ```

##Our optimization proposal

In our optimization function, we will find the optimal placements of trash collection units. When running the algorithm, the user has the option to specify the number of trash cans they are willing to install as well as the radius they expect a single collection unit to cover. The algorithm returns a list of location coordinates that fit these criteria.

###How to run:
```
$ python3 optimization.py
```

Note: By default the algorithm chooses 5 trash cans with a 200 meter radius. Trial mode is also on. These parameters can be changed in the code. In addition, if our algorithm is run using the Jupyter Notebook, it will also output a graph of service requests, code violations, big belly locations, and the proposed placement of new trash collection units. (This code has been commented out so that the algorithm can run in a normal Python 3 shell)

## Algorithm Explanation

We will look at the geolocations of sanitary violations & requests (which we found via serviceRequests.py and codeViolations.py), the weights of those locations (the amount of violations corresponding to that location), and we will also look at the locations of Big Belly's and their average fullness (which we found via bigBelly.py). In addition, the algorithm will look at the geolocation of pre-existing trash collection units (found via trashSch.py) and will focus more on areas where trash is collected less regularly. Based upon these datasets we will find the optimal placement of trash collection units to reduce the overall amount of violations associated with excess waste. We will find these placements using a weight metrics and KD trees. The user will be able to change the number of trashcans and the radius that each trashcan can cover as parameters to our 'findOptimalLocation' function.

###Our weight metrics are as follows:

For code violations, overfilling or heavy amounts of trash were weighted higher. Incidents indirectly associated with excessive trash were weighted lower, such as 'insects/animals'. For each location we calculate the weight using count * the scale given below.
* improper storage trash - 0.75
* illegal dumping - 0.75
* overfilling of barrel/ dumpster - 1
* storage of garbage & rubbish - 0.75 
* insects rodents animals - 0.3
* trash illegally dump container - 0.75

For service requests, trash specific requests were weighted higher. Incidents indirectly associated with excessive trash were weighted lower, such as 'pest infestation'. For each location we calculate the weight using count * the scale given below.
* illegal dumping - 0.75
* improper storage of trash (barrels) - 0.75
* mice infestation - residential -> 0.3
* missed trash/recycling/yard waste/bulk item - 0.5
* overflowing or un-kept dumpster - 1
* pest infestation - residential -> 0.3
* rodent activity - 0.5
* unsanitary conditions - establishment -> 0.3

For Big Belly's, units with high average fullness were weighted higher, but we also weighted Big Belly's lower in general because they appear to be concentrated in specific areas and we did not want the this dataset alone to skew our data one way or another. For each location we calculate the weight using count * the scale given below.
	range of avg fullness -> weighted value
* 0.9 - 1.0 -> 0.3 
* 0.7 - 0.89 -> 0.25
* 0.5 - 0.69 -> 0.2
* 0.3 - 4.9 -> 0.15
* 0.0 - 0.29 -> 0.1

For Trash Schedules, we add a negative weight to areas that have multiple collection days. Our reasoning is these areas already have focused effort in terms of time and resources devoted to collecting trash. We want the areas that are not receiving as much attention to be weighted higher. For each location we calculate the weight using count * the scale given below.
* -1 * number of times trash was collected at this location.

## K-D trees

For each trash collection unit the user wants to install, we choose x random coordinates (x is specified from the iterations parameter when the optimized function is called) from a master dataset that contains all entries from the datasets mentioned before. 
We then use python's K-D tree library to find all points within a certain radius of these random points. Using the points within these radii we calculate the total weight of the region associated with that coordinate. The algorithm will find the location coordinates of the regions that have maximal weight out of all randomly selected coordinates. 

To ensure the algorithm does not return the same regions for every iteration, if the algorithm is searching for multiple trash collection units to install it will 'ignore' regions that have already been selected by previous iterations.















