#Project Part 1
We are interested in determining whether or not children dedicated establishments in Boston have less drug crime incidents. We plan on analyzing the proximity of the drug crime incidents to all public schools, private schools, day camps, child feeding programs, private daycares and public daycares. With this data, we can create visualizations in the form of histograms whose data can confirm if there is a definite correlation between any of these children establishments and drug crimes.

#Data Sets
Legacy Crimes: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx

Current Crimes: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap

Public Schools: http://bostonopendata.boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0

Private Schools: http://bostonopendata.boston.opendata.arcgis.com/datasets/0046426a3e4340a6b025ad52b41be70a_1

Public Daycares: https://data.cityofboston.gov/dataset/City-Day-Care/q6h3-7rpz/alt

Private Daycares: https://www.care.com/day-care/boston-ma

Day Camps: https://data.cityofboston.gov/dataset/Day-Camps/sgf2-btru

Children Feeding Programs: https://data.cityofboston.gov/Human-Services/Children-s-Feeding-Program/p9yd-36dn

To get these datasets: $ python3 getData.py

(getData.py calls the python module scrapePrivateDaycares.py which requires selenium. To run the webscraper, download the Chrome webdriver at: https://sites.google.com/a/chromium.org/chromedriver/downloads and on line 13 of scrapePrivateDaycares.py please make sure to input the path to the downloaded chrome driver as an argument for the webdriver. Example: driver = webdriver.Chrome("C:/Users/Test/chromedriver.exe"))

#Transformations
Transformation 1: The first transformation merges all the Legacy and Current crimes data sets. This will create a consolidated dataset that merges the old and new crime datasets from the city of Boston. We will be only selecting the locations of the crimes and merging them on their type of crime. The resulting dataset will be a consolidated dataset of all the crimes in Boston from July 2012 to current with their locations and whether or not they are a related crime. This creates the allCrimesMaster dataset.

To run this script: $ python3 mergeAllCrimes.py

Transformation 2: The second transformation is similar to the first except it only grabs the crimes that are not drug related and puts them in their own dataset. This creates the noDrugCrimesMaster dataset.

To run this script: $ python3 mergeAllWithoutCrimes.py

Transformation 3: This third tranformation is also similar to the first. It only grabs the crimes that are drug related. This creates the allDrugCrimesMaster dataset.

To run this script: $ python3 mergeDrugCrimes

Transformation 4: This tranformation merges the public school and private school datasets. This will select the school name and location while preserving if the school is private or public. Because some of the latitue and longitude values were unavailable, their latitudes and longitudes were converted from their physical adresses using the geopy package. This creates the schoolsMaster dataset.

To run this script: $ python3 mergeSchools.py

Transofmation 5: This transformation merges the day camps, public daycares and private daycares datasets while just selecting certain columns from the child feeding programs dataset. It merges the day camps, public daycares and private daycares and preserves name, type of establishment and location. Location and name are selected from the child feeding programs dataset. This script creates the childFeedingProgramsTrimmed and dayCampdayCaresMaster datasets.

To run this script: $ python3 mergeChildren.py

#Project part 2

We intend to see a correlation exists between certain establishments associated with children and proximity to drug crimes. Pulling data from the Private Schools, Public Schools, Child Feeding Programs, Day Camps, Public Daycares, Private Daycares, and Crimes data sets gives us the location in address or latitude and longitude format of an establishment or crime. We intend to then take each crime, drug related or not, and find the frequency of each type of establishment within a specific distance/radius to the crime. Then we take each drug related crime and find their frequencies within the same distance. Then we want to compare these two by plotting frequencies for each type of establishment on histograms. The goal is to find, while comparing the histogram with all crimes against the histogram with only drug crimes, a contrast in the peaks of the histograms. This will point to a correlation between the specific type of establishment and drug crimes. We also intend to implement an optimization function to optimize the size of the radius which dictates how many establishments will be within range to be counted in the frequencies. The optimization function will give us a specific distance which will create the largest or most obvious difference in peaks on the histograms we create.

#Run Instructions
Instead of having to run the scripts individually, run getDataAndMerge.py followed by getData3.py.

#Analysis
prepData1.py: This script takes an argument r and the allCrimesMaster, allDrugCrimesMaster, childFeedingProgramsTrimmed, dayCampdayCaresMaster and schoolsMaster datasets. Then, using r as a distance, takes every single crime in the allCrimesMaster data set and finds the frequency of the different types of establishments found within that distance. These frequencies are then put into the numberOfEstablishmentsinRadius dataset. The same thing is done to the allDrugCrimesMaster dataset but creates the numberOfEstablishmentsinRadiusDrug dataset

To run this script: $ python3 prepData1.py
Note: execute function is commented out here to allow for function call with parameter by prepData3.py

prepData2.py: This script takes the two datasets created by prepData1.py. It implements a map reduce function on each that returns a distribution of the number of crimes that have x children establishments within the specified proximity from prepData1. It will also have a product of the crimes by establishments with an appended temporary variable used to collapse the data in the reduce function. Another map reduce is applied on the resulting datasets of the above map reduce to produce the total sum of establishments around each crime and the total sum of crimes. Using these values, the average number of establishments around each crime is calculated.

To run this script: $ python3 prepData2.py

prepData3.py: This script functions as a wrapper around prepData1 and prepData2. It iterates through a range of distances to pass to prepData1. It will execute prepData1 with the specified distance, then execute prepData2, and finally calculate the averages of the frequencies of all crimes near the specified establishments compared to just drug crimes near the specified establishments. Using the data here, we are capable of pin pointing the optimal distance to use when running a linear regression on the proximities. 

To run this script: $ python3 prepData3.py

#Results

Optimization Function:
value of d: 1
avg_all: 36.30665612960585
avg_drug: 38.260169491525424
diff: -1.9535133619195761

value of d: 2
avg_all: 119.08479304553985
avg_drug: 122.2272033898305
diff: -3.142410344290653

value of d: 3
avg_all: 222.20275017287366
avg_drug: 223.4477966101695
diff: -1.2450464373

value of d: 4
avg_all: 322.10269682900326
avg_drug: 320.82872881355934
diff: 1.273968015443927

value of d: 5
avg_all: 405.74762817346635
avg_drug: 403.215
diff: 2.532628173466379

value of d: 6
avg_all: 465.5048661463993
avg_drug: 463.62127118644065
diff: 1.88359495996

With regards to the optimization results, we chose to work with a value of 2 miles when finding the number of children associated establishments around each crime. This was because it was at this integer that the average number of establishments around each drug crime was much greater than that around each crime. In the future, we plan to find the optimal distance by testing distances of 0.1 mile increments.

![alt tag](https://raw.githubusercontent.https://github.com/tyao123/course-2016-fal-proj/tree/master/aditid_benli95_teayoon_tyao)














