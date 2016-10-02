# Project Proposal

We propose to answer the question of whether or not a neighborhood is safe. Users will be able to enter either a neighborhood or a street name and our program will return data regarding overall safety of the area. This includes how well lit an area is, the crime rate (including specifics about the crime), overall cleanliness of the area, and upkeep of the area. In order to do this, we would use the "Master Address List" dataset and return data that is relevant to the area. We would output data in a way that makes it easy for an average citizen to get an idea of how safe an neighborhood is.

## Data Sets

* Street Light Locations: https://data.cityofboston.gov/Facilities/Streetlight-Locations/7hu5-gg2y
* 311 Reports: https://data.cityofboston.gov/City-Services/311-Service-Requests/awu8-dc52
* Crime Incident Reports: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap
* Approved Building Permits: https://data.cityofboston.gov/Permitting/Approved-Building-Permits/msk6-43c6
* Master Address List: https://data.cityofboston.gov/City-Services/Master-Address-List/t85d-b449

## Retrieve and Combine Data

1. Run approvedBuildingPermits.py, crime.py, masterAddress.py, streetLights.py, and threeOnOne.py. This will retrieve all data and store the information in MongoDB.

2. To combine datasets, you will need to run three files. Run streetLightCrimes.py to combine Street Light Locations and Crime Incident Reports. Run maintenance.py to combine Closed 311 Service Requests and Approved Building Permits. Run addressStreetLights.py to combine the Master Address List and Street Light Locations. The first combination will get the number of street lights within the area of a specific crime. The second combination will combine the approved building permits and closed 311 requests to later help us measure the amount of maintenance in a certain area. The third combination will get the number of street lights around a certain address. 

Scripts can be run using:
```
$ python3 [name of file].py
```

