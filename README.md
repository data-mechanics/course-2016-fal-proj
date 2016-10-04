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

1. Run gatherDataSets.py in order to retrieve and store all required raw data

2. Run the other transformation scripts (order doesn't matter):
    
    bigBelly.py:
    Uses "Big Belly Alerts 2014" dataset and performs transformations to find the amount of big belly compactors and the overall average fullness of Boston geolocations.

    How to run:
    ```
    $ python3 bigBelly.py
    ```

    codeViolations.py:
    Uses "Code Enforcement" and "Food Establishments Inspections" datasets and performs transformations to find health/sanitation related violations and the number of times they occured within each Boston zipcode.

    How to run:
    ```
    $ python3 codeViolations.py
    ```

    serviceRequests.py:
    Uses "Mayors 24 Hour Hotline" dataset and performs transformations to find the types of santiation-related requests and the number of times it was request for each Boston zipcode.

    How to run:
    ```
    $ python3 serviceRequests.py
    ```

    trashSchedules.py:
    Uses "Master Address List" dataset and performs transformations to find the trash collection days for each Boston zipcode. 

    How to run:
    ```
    $ python3 trashSchedules.py
    ```














