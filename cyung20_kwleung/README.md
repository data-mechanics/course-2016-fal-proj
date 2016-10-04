#Project Proposal

Whether it's commuting to work/school, going home, or going out with friends, the T is used by a myriad of people who rely on it as their main source of transportation.
For our project, we intend to compare and determine which T stops are the safest at night. We will be looking at Crime Incident Reports (made between August 2015 to now) around each T stop, as well as other datasets/variables that might have an influence on the frequency of crimes in particular areas. 

#Datasets Being Used
**1. MBTA T Stop Locations**

JSON file containing every MBTA T stop with their station names, coordinates (longitude + latitude), URL to their MBTA information pages, which line they belong to, and addresses. Data taken from http://erikdemaine.org/maps/mbta/, and stored on datamechanics.io for retrieval.<br>
**URL : http://datamechanics.io/data/cyung20_kwleung/mbta-t-stops.json**

```
$ python3 mbtaLocations.py
```

**2. Crime Incident Reports (August 2015 - Date)**

This will be used to find out how many crimes were reported near each T stop.<br>
**URL : https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap**

```
$ python3 crimeReports.py
```

**3. Liquor Licenses** 

This will be used to determine whether or not crimes were more likely to transpire given they were reported near businesses that sell liquor.<br>
**URL: https://data.cityofboston.gov/dataset/Liquor-Licenses/hda6-fnsh**

```
$ python3 liquorLicense.py
```

**4. Streetlight Locations**

This will be used to compare crimes reported near T stops and see if the number of streetlights near said stops had an influence.<br>
**URL: https://data.cityofboston.gov/Facilities/Streetlight-Locations/7hu5-gg2y**

```
$ python3 streetlightLocations.py
```

**5. Boston Police District Stations**

This will be used to determine whether or not crimes were more likely to transpire given how far each T stop was to its nearest Boston Polic District Station.<br>
**URL: https://data.cityofboston.gov/Public-Safety/Boston-Police-District-Stations/23yb-cufe**

```
$ python3 BPDStations.py
```
