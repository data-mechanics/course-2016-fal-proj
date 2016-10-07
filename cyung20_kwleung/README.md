#Project Proposal

For our project, we will be looking at Crime Incident Reports, as released by the City of Boston, and linking them to various other datasets to see if we can determine how safe areas might be based on the businesses, public services, etc. that encompass these areas. This may be used by anyone to estimate how safe they may be wherever they are, provided they are near one of the variables we analyze. In particular, we will be looking at the following datasets: Crime Incident Reports (made between August 2015 to now); businesses that have a liquor license; locations of streetlights; MBTA T Stop Locations; and the locations of all Boston Police District Stations.

#Datasets Being Looked At
**1. MBTA T Stop Locations**

JSON file containing every MBTA T stop with their station names, coordinates (longitude + latitude), URL to their MBTA information pages, which line they belong to, and addresses. Data taken from http://erikdemaine.org/maps/mbta/, and stored on datamechanics.io for retrieval.<br>
**URL : http://datamechanics.io/data/cyung20_kwleung/mbta-t-stops.json**

**2. Crime Incident Reports (August 2015 - Date)**

Dataset of Crime Incident Reports.<br>
**URL : https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap**<br>
**API Endpoint: https://data.cityofboston.gov/resource/29yf-ye7n.json**

**3. Liquor Licenses** 

This will be used to see if crimes were more likely to occur near businesses that hold liquor licenses.<br>
**URL: https://data.cityofboston.gov/dataset/Liquor-Licenses/hda6-fnsh**<br>
**API Endpoint: https://data.cityofboston.gov/resource/g9d9-7sj6.json**

**4. Streetlight Locations**

This will be used to see if the number of streetlights in an area has an effect on the number of crimes in said area.<br>
**URL: https://data.cityofboston.gov/Facilities/Streetlight-Locations/7hu5-gg2y**<br>
**API Endpoint: https://data.cityofboston.gov/resource/fbdp-b7et.json**

**5. Boston Police District Stations**

This will be used to see how often crimes were committed near Boston Police District Station.<br>
**URL: https://data.cityofboston.gov/Public-Safety/Boston-Police-District-Stations/23yb-cufe**<br>
**API Endpoint: https://data.cityofboston.gov/resource/pyxn-r3i2.json**

To retrieve information from each dataset, run mbtaLocations.py, crimeReports.py, liquorLicenses.py, streetlightLocations.py, and BPDStations.py, respectively.

#Transformations

**1. liquorAndCrime.py**

This transformation. takes the product of 1) a set containing the name of the liquor store and its location in longitude and latitude and 2) a set containing the location of a crime. Selection is used to make sure that the essential data from liquor stores (location, name) that are located 25 meters or less from a location in which a crime was reported committed are included in the new data set. There is a double count. 

**2. liquorAndBPDS.py**

This transformation looks for liquor stores that do not have a Boston Police District Station within 1 mile (around 1609 meters). This transformation may be useful in trying to determine if there is a correlation between crimes reported/committed and proximity of Boston Police District Stations.

**3. crimesNearBPDS.py**

This algorithm retrieves the districts in which crime incidents have been reported (as recorded by the Crime Incident Report dataset), counts how many crimes transpired in each individual district, then adds the counts to the pre-existing BPDS database with their corresponding districts.
