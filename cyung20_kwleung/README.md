#Project Proposal

Whether it's commuting to work/school, going home, or going out with friends, the T is used by a myriad of people who rely on it as their main source of transportation who ideally want to go from one place to another without ever getting into harm's way. 
For our project, we intend to compare and determine which T stops are the safest while in consideration of various factors that may have an influence on the frequency of crimes near said T stops. In particular, we will be looking at the following datasets: Crime Incident Reports (made between August 2015 to now); businesses that have a liquor license; locations of streetlights; and the locations of all Boston Police District Stations.

#Datasets Being Used
**1. MBTA T Stop Locations**

JSON file containing every MBTA T stop with their station names, coordinates (longitude + latitude), URL to their MBTA information pages, which line they belong to, and addresses. Data taken from http://erikdemaine.org/maps/mbta/, and stored on datamechanics.io for retrieval.<br>
**URL : http://datamechanics.io/data/cyung20_kwleung/mbta-t-stops.json**

**2. Crime Incident Reports (August 2015 - Date)**

This will be used to find out how many crimes were reported near each T stop.<br>
**URL : https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap**<br>
**API Endpoint: https://data.cityofboston.gov/resource/29yf-ye7n.json**

**3. Liquor Licenses** 

This will be used to determine whether or not crimes were more likely to transpire given they were reported near businesses that sell liquor.<br>
**URL: https://data.cityofboston.gov/dataset/Liquor-Licenses/hda6-fnsh**<br>
**API Endpoint: https://data.cityofboston.gov/resource/g9d9-7sj6.json**

**4. Streetlight Locations**

This will be used to compare crimes reported near T stops and see if the number of streetlights near said stops had an influence.<br>
**URL: https://data.cityofboston.gov/Facilities/Streetlight-Locations/7hu5-gg2y**<br>
**API Endpoint: https://data.cityofboston.gov/resource/fbdp-b7et.json**

**5. Boston Police District Stations**

This will be used to determine whether or not crimes were more likely to transpire given how far each T stop was to its nearest Boston Police District Station.<br>
**URL: https://data.cityofboston.gov/Public-Safety/Boston-Police-District-Stations/23yb-cufe**<br>
**API Endpoint: https://data.cityofboston.gov/resource/pyxn-r3i2.json**

To retrieve information from each dataset, run mbtaLocations.py, crimeReports.py, liquorLicenses.py, streetlightLocations.py, and BPDStations.py, respectively.

#Transformations

1.

2.

3.
