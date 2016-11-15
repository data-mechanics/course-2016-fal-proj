#Project Proposal

For our project, we will be looking at Crime Incident Reports, as released by the City of Boston, and linking them to various other datasets to see if we can determine how safe areas might be based on the businesses, public services, etc. that encompass these areas. This may be used by anyone to estimate how safe they may be wherever they are, provided they are near one of the variables we analyze. In particular, we will be looking at the following datasets: Crime Incident Reports (made between August 2015 to now); businesses that have a liquor license; locations of streetlights; MBTA T Stop Locations; and the locations of all Boston Police District Stations.

Below are the following techniques we have implemented in order to further address our problem:

1) Our implementation of constraint satisfaction takes a select number of offenses (from the crime incident reports dataset) and counts how many have been reported within 100 meters of all the liquor stores from the liquor license dataset. We will also analyze how frequent these offenses occur during the weekdays (ie. Monday, Tuesday, Wednesday, and Thursday) compared to during the weekends (Friday, Saturday, and Sunday) - with Friday being grouped into the weekend because people typically have no obligations the next day so they are more free. This will ultimately help give us an idea of what crimes are most frequent around liquor stores (more will be added and analyzed later) and if it's more dangerous around liquor stores during the weekday or during the weekend.

2) A statistical analysis we thought about doing is seeing whether the time of day might have an influence on when a crime is committed. We began by counting the number of crimes (from the crime incident reports dataset) that occurred at every hour of the day. For reference, we recorded the hours in military time (ie. from our hour range 0-23: 0 represents 12 AM, 12 represents 12 PM, 17 represents 5 PM, etc). After compiling the results into two vectors (with the hours in one and each hours' corresponding crime counts in the other), we proceeded to calculate their correlation coefficient in order to determine how strong of a correlation the two variables have.

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

**NOTE: This python file has been modified and no longer implements the following algorithm. Below is a description of an updated version of liquorAndCrime.py**<br>
This transformation takes the product of 1) a set containing the name of the liquor store and its location in longitude and latitude and 2) a set containing the location of a crime. Selection is used to make sure that the essential data from liquor stores (location, name) that are located 25 meters or less from a location in which a crime was reported committed are included in the new data set. There is a double count. 

**2. liquorAndBPDS.py**

This transformation looks for liquor stores that do not have a Boston Police District Station within 1 mile (around 1609 meters). This transformation may be useful in trying to determine if there is a correlation between crimes reported/committed and proximity of Boston Police District Stations.

**3. crimesNearBPDS.py**

This algorithm retrieves the districts in which crime incidents have been reported (as recorded by the Crime Incident Report dataset), counts how many crimes transpired in each individual district, then adds the counts to the pre-existing BPDS database with their corresponding districts.

#Constraint Satisfaction and Statistical Analysis

**1. liquorAndCrime.py** 
We have modified our version of liquorAndCrime.py to now include constraints which we deem necessary towards addressing our problem. It uses selection to make sure essential information from liquor stores and crimes are stored in the repository. One major update was that their were constraints added. After making sure liquor stores and crime locations within 100 meters are stored (as opposed to 25 meters earlier), we filter out the crimes by having the constraints be these type of crimes: "Harassment" "Aggravated Assault" "Simple Assault." We are taking a small subset of the crimes by doing so. Our justification for implementing this is that we want to see if crimes pertaining to physical abuse and/or directly and personally impactful to others in a negative way is correlated with the crime scene being within 100 meters of a liquor store or not. 

**2. crime_time_correlation.py**
Stastical Analysis was used to determine if there is a correlation between crimes and the hour they were committed. After calculations, we received a correlation coefficient with a magnitude of 0.589, which tells us that the two variables have a positive, moderately strong correlation.
