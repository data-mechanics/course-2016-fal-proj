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

#Analysis
prepData1.py: This script takes an argument r and the allCrimesMaster, allDrugCrimesMaster, childFeedingProgramsTrimmed, dayCampdayCaresMaster and schoolsMaster datasets. Then, using r as a distance, takes every single crime in the allCrimesMaster data set and finds the frequency of the different types of establishments found within that distance. These frequencies are then put into the numberOfEstablishmentsinRadius dataset. The same thing is done to the allDrugCrimesMaster dataset but creates the numberOfEstablishmentsinRadiusDrug dataset

To run this script: $python3 prepData1.py