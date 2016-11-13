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

##Our optimization proposals

1. We follow the example of finding the optimal placement of a hospital to be shared among n neighborhoods. We will look at the geolocations of sanitary violations & requests (which we found via serviceRequests.py and codeViolations.py), the weights of those locations (the amount of violations corresponding to that location), and we will also look at the locations of Big Belly's and their average fullness (which we found via bigBelly.py) and based upon these two datasets we will find the optimal placement of trash collection sites to reduce the overall amount of violations associated with excess waste. We will find these placements using a weight metric and KD trees. The user will be able to change the number of trashcans and the radius that each trashcan can cover as parameters to our 'findOptimalLocation' function.

Our weight metric is as follows:
For code violations we weighted violations specific to overfilling or heavy amounts of trash higher. 
	-weighted by count x scale category of request 
	-scale:
		-Improper storage trash - 0.75
		-illegal dumping - 0.75
		-overfilling of barrel/ dumpster - 1
		-storage of garbage & rubbish - 0.75 
		-insects rodents animals - 0.3
		-trash illegally dump container - 0.75

For service requests we weighted trash specific requests higher.
	-weighted by count x scale category of request
	-scale:
		-illegal dumping - 0.75
		-improper storage of trash (barrels) - 0.75
		-mice infestation - residential -> 0.3
		-missed trash/recycling/yard waste/bulk item - 0.5
		-overflowing or un-kept dumpster - 1
		-pest infestation - residential -> 0.3
		-rodent activity - 0.5
		-unsanitary conditions - establishment -> 0.3

For Big Belly's we weighted units with high average fullness higher, but we also weighted Big Belly's lower in general because they appear to be concentrated in specific areas to begin with and we did not want the Big Belly's alone to skew our data one way or another.

	-weighted by range of average fullness 

	floor - ceiling -> weighted value
		0.9 - 1.0 -> 0.3 
		0.7 - 0.89 -> 0.25
		0.5 - 0.69 -> 0.2
		0.3 - 4.9 -> 0.15
		0.0 - 0.29 -> 0.1

For Trash Schedules, we add a negative weight to areas that have multiple collection days. Our reasoning is these areas already have focused effort in terms of time and resources devoted to collecting trash. We want to weight higher the areas that are not receiving as much attention.

	-weighted using the geolocation coordinates as a key and the collection day as a value
	-metric: -1 * number of times trash was collected at this location.
	-by making these weights negative, our algorithm will know to focus less on areas that already have high trash collection activity.


(one bigbelly = $$$, to ensure a budget of <= $$$ we set limit arbitrarily at X)

2. We will perform a statistical analysis on the dataset produced by trashSch.py to see if locations with multiple collection days correspond with or are nearby locations that our optimization in part 1 found. We could then propose that our optimization would indeed positively affect locations with heavy trash output/ violations by adding additional trash collection sites there. In addition, this could possibily expose the need to reevaluate current trash collection schedules.














