# Introduction

According to the Boston Globe, “Major crime in Boston dropped 9 percent between 2014 and 2015, bringing the figure to its lowest point in a decade.” While the diminishing crime rate is certainly an accomplishment for Boston, it is still necessary to analyze the effect certain factors have on crime incidences. As stated by the US National Library of Medicine, “place-based” factors are particularly useful for evaluating crime in that “states and cities can build on such information to strengthen their alcohol control and policing policies.” Therefore in order to evaluate crime incidence in Boston, we believe looking at the influencers that are most commonly associated with crimes will help confirm whether people have an actual need to worry about them.

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

This program was written initially to [delete below and add explanation of what it does]
< This transformation takes the product of 1) a set containing the name of the liquor store and its location in longitude and latitude and 2) a set containing the location of a crime. Selection is used to make sure that the essential data from liquor stores (location, name) that are located 25 meters or less from a location in which a crime was reported committed are included in the new data set. There is a double count. >

**Transformation #2: liquorAndBPDS.py**

This transformation looks for liquor stores that do not have a Boston Police District Station within 1 mile (around 1609 meters) of them. This transformation was written to potentially determine if there is a correlation between crimes committed and their relative proximities to Boston Police District Stations.

**Transformation #3: crimesNearBPDS.py**

This algorithm retrieves the districts in which crime incidents have been reported (as recorded by the Crime Incident Report dataset), and counts how many crimes transpired in each individual district, then adds the counts to the pre-existing BPDS database with their corresponding districts.

**Statistical Analysis #1: liquorAndCrime.py** 

Built off of the previous transformation liquorAndCrime.py, this algorithm now implements a constraint satisfaction that takes a select number of offenses (from the crime incident reports dataset) and counts how many have been reported within 100 meters of all the liquor stores from the liquor license dataset. It also analyzes how frequent these offenses occur during the weekdays (ie. Monday, Tuesday, Wednesday, and Thursday) compared to during the weekends (Friday, Saturday, and Sunday) - with Friday being grouped into the weekend because people typically have no obligations the next day so they are more free to stay out as long as they desire. We used this to gather an idea of what crimes are most frequent around liquor stores (more will be added and analyzed later) and if it's more dangerous around liquor stores during the weekday or during the weekend. In particular, we picked crime categories that are specifically and directly malicious to civilians such as "Sex Offender Registration" and "Harrassment" and "Homocide" for analysis.

**Statistical Analysis #2: crime_time_correlation.py**

This algorithm was written with the intent of seeing whether the time of day might have an influence on when a crime is committed. We began by counting the number of crimes (from the crime incident reports dataset) that occurred at every hour of the day. For reference, we recorded the hours in military time (ie. from our hour range 0-23: 0 represents 12 AM, 12 represents 12 PM, 17 represents 5 PM, etc). After compiling the results into two vectors (with the hours in the first and each hours' corresponding crime counts in the other), this program calculates the vectors' correlation coefficient in order to determine how strong of a correlation the two variables have.

# Results

**Transformation #3: crimesNearBPDS.py Results:**

![alt_text](https://github.com/CalvinYL/course-2016-fal-proj/blob/master/cyung20_kwleung/crimesNearBPDS%20Table.png)

**Statistical Analysis #1: liquorAndCrime.py Results:**

![alt text](https://github.com/CalvinYL/course-2016-fal-proj/blob/master/cyung20_kwleung/liquorAndCrime%20Table.png)

**Statistical Analysis #2: crime_time_correlation.py Results:**

![alt_text](https://github.com/CalvinYL/course-2016-fal-proj/blob/master/cyung20_kwleung/visualization/Image%20of%20lin_reg.PNG)

# Conclusion

From our results, while we cannot state any facts definitively, we may, however, confirm that there is certainly a moderately strong correlation between crimes and the usual factors they’re associated with (ie. time of day in which crimes transpire most and frequency (on average) throughout the week near locations they are more prone to occur). With regards to our results in our first image, we cannot confidently make any direct claims without knowing more about each district (ie. how many resources are allocated to each station, how many police officers each station has, etc.), but it is interesting to observe how districts with jurisdiction over smaller populations don’t necessarily have fewer crimes (and the same goes with larger populations). 
