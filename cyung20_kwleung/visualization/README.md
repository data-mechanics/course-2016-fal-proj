# CS591 Data Mechanics Final Report

# Introduction

According to the Boston Globe, “major crime in Boston dropped 9 percent between 2014 and 2015, bringing the figure to its lowest point in a decade.” While the diminishing crime rate is certainly an accomplishment for Boston, it is still necessary to analyze the effect certain factors have on crime incidences. As stated by the US National Library of Medicine, “place-based” factors are particularly useful for evaluating crime in that “states and cities can build on such information to strengthen their alcohol control and policing policies.” Therefore in order to evaluate crime incidence in Boston, we believe looking at the influencers that are most commonly associated with crimes will help confirm whether people have an actual need to worry about them when they're out and about.

# Datasets

For our project, we looked closely at the datasets below - all taken from the [City of Boston Database](https://data.cityofboston.gov/) - to determine if we could find any links between them:

**1. Crime Incident Reports (August 2015 - Date)**

**URL : https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap**<br>
**API Endpoint: https://data.cityofboston.gov/resource/29yf-ye7n.json**

**2. Liquor Licenses** 

**URL: https://data.cityofboston.gov/dataset/Liquor-Licenses/hda6-fnsh**<br>
**API Endpoint: https://data.cityofboston.gov/resource/g9d9-7sj6.json**

**3. Boston Police District Stations**

**URL: https://data.cityofboston.gov/Public-Safety/Boston-Police-District-Stations/23yb-cufe**<br>
**API Endpoint: https://data.cityofboston.gov/resource/pyxn-r3i2.json**

# Algorithms Used - Transformations and Analyses

We performed three transformations in order to prepare our data for the two statistical analyses below:

**Transformation #1: liquorAndCrime.py**

This program was written initially to look for crime incidents that are close enough to one or more liquor store (25 meters in our case) to be potentially significant and to keep the data of both the crime and the liquor store. This transformation takes the product of 1) a set containing the name of the liquor store and its location in longitude and latitude and 2) a set containing the location of a crime. Selection is used to make sure that the essential data from liquor stores (location, name) that are located 25 meters or less from a location in which a crime was reported committed are included in the new data set. There is a double count. 

**Transformation #2: liquorAndBPDS.py**

This transformation looks for liquor stores that do not have a Boston Police District Station within 1 mile (around 1609 meters) of them. This transformation was written to potentially determine if there is a correlation between crimes committed given they are within the vicinity of a Boston Police District Station.

**Transformation #3: crimesNearBPDS.py**

This algorithm retrieves the districts in which crime incidents have been reported (as recorded by the Crime Incident Report dataset), and counts how many crimes transpired in each individual district. It then adds the counts to the pre-existing BPDS database with their corresponding districts.

**Statistical Analysis #1: liquorAndCrime.py** 

Built off of the previous transformation liquorAndCrime.py, this algorithm now implements a constraint satisfaction that takes a select number of offenses (from the crime incident reports dataset) and counts how many have been reported within 100 meters of all the liquor stores from the liquor license dataset. It also analyzes how frequent these offenses occur during the weekdays (ie. Monday, Tuesday, Wednesday, and Thursday) compared to during the weekends (Friday, Saturday, and Sunday) - with Friday being grouped into the weekend because people typically have no obligations the next day so they are more free to stay out as long as they desire. We used this to gather an idea of what crimes are most frequent around liquor stores, and if it's more dangerous around liquor stores during the weekday or during the weekend. In particular, we picked crime categories that are specifically and directly malicious to civilians such as "Sex Offender Registration" and "Harrassment" and "Homocide" for analysis.

**Statistical Analysis #2: crime_time_correlation.py**

This algorithm was written with the intent of seeing whether the time of day might have an influence on when a crime is committed. We began by counting the number of crimes (from the crime incident reports dataset) that occurred at every hour of the day. For reference, we recorded the hours in military time (ie. from our hour range 0-23: 0 represents 12 AM, 12 represents 12 PM, 17 represents 5 PM, etc). After compiling the results into two vectors (with the hours in the first and each hours' corresponding crime counts in the other), this program calculates the vectors' correlation coefficient in order to determine how strong of a correlation the two variables have.

# Results

**Transformation #3: crimesNearBPDS.py Results:**

![alt_text](https://github.com/CalvinYL/course-2016-fal-proj/blob/master/cyung20_kwleung/crimesNearBPDS%20Table.png)

**Statistical Analysis #1: liquorAndCrime.py Results:**

![alt text](https://github.com/CalvinYL/course-2016-fal-proj/blob/master/cyung20_kwleung/liquorAndCrime%20Table.png)

**Statistical Analysis #2: crime_time_correlation.py Results:**

![alt_text](https://github.com/CalvinYL/course-2016-fal-proj/blob/master/cyung20_kwleung/visualization/Image%20of%20lin_reg.PNG)

**Leaflet Map: Districts, BPDS and Crime**

![alt_text](https://github.com/CalvinYL/course-2016-fal-proj/blob/master/cyung20_kwleung/visualization/image_of_CrimeAndDistricts.png)


# Conclusion

From our results, while we cannot state any facts definitively (since correlation does not mean causation), we may, however, confirm that a relationship does exist between crimes and the usual factors they’re associated with.<br>
For our first analysis, the averge number of crimes committed over the span of the weekend is greater than the average crimes committed over the span of the weekdays - which was expected.<br>
For our second analysis/first visualization, we created a graph showing the different hours against the number of crimes that occurred during each hour. We also calculated a correlation coefficient of 0.59, which tells us that the relationship between the two variables are positive and moderately strong. While it made sense that crimes were lowest in the early morning, it was interesting to observe that crime rates were generally high from the afternoon to late evening.<br>
With regards to our results in our first image produced by our third transformation, we cannot confidently make any direct claims without knowing more information about each district (ie. how many resources are allocated to each station, how many police officers each station has, etc.), but it is interesting to observe how districts with jurisdiction over smaller populations don’t necessarily have fewer crimes (similarly, the same can be said about larger populations not necessarily having higher numbers of crimes). In addition, our other visualization is a map created through Leaflet which shows every Boston district and every Boston Police District Station. More crime-infested districts are colored darker and users may click on the district to find the district name and the corresponding number of crimes that occured within that district. Because all the crimes from the data set were accounted for, rather than just a subset of 10,000, this map more accurately reflects the crime per district in comparison to our first Statistical Analysis. Our map reaffirms our observation from Statistical Analysis #1 that the size of the district may not be correlated with the amount of crime in a district, although the map gives us a good sense of which areas actually have the most crimes. One interesting observation we noticed is that The Boston Police Headquarters and District B-2 Police Station are both located in District B-2, although District B-2 has the highest counts of crime total. Most districts only have one police station, but the number of crimes varies between them vastly. Our map shows that districts closer to the center of Boston are shaded darker and thus have more crimes while those on the sides and outliers are lighter and have fewer crimes. While districts B2 and C11 have the most total crimes, the number of total counts is not the only factor in determining which area is the safest or dangerous, and thus more research regarding the types of crimes and different regions within each district is needed. 

It's obvious that there are a countless number of other factors which could be tested to further strengthen our project (such as checking the general safeness in areas with mutliple street lights or common areas such as public transporation stops at different times of the day or days of the week) and that this project will never really have a permanent solution. However, the more we analyze the different crime influencers, the more measures can be taken to, at the very least, decrease criminal activity in Boston.

# Citations

1) https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3673267/<br>
2) https://www.bostonglobe.com/metro/2016/01/15/major-crime-drops-boston/TrO5ZAhmOD3bFDqdBX8vwN/story.html<br>
3) https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap<br>
4) https://data.cityofboston.gov/dataset/Liquor-Licenses/hda6-fnsh<br>
5) https://data.cityofboston.gov/Public-Safety/Boston-Police-District-Stations/23yb-cufe<br>
6) http://worldmap.harvard.edu/data/geonode:boston_police_districts_f55<br>
7) https://en.wikipedia.org/wiki/Boston_Police_Department<br>
