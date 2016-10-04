#Project
We are interested in determining whether or not children dedicated establishments in Boston have less drug crime incidents. We plan on analyzing the proximity of the drug crime incidents to all Boston schools, food pantries, children feeding programs, and day camps. Some other attributes that we plan to track are whether or not the school is public and the day of the week that the drug crime incident occurs on. 

#Data Sets
Legacy Crimes: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx
Current Crimes: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap
Public Schools: http://bostonopendata.boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0
Private Schools: http://bostonopendata.boston.opendata.arcgis.com/datasets/0046426a3e4340a6b025ad52b41be70a_1
Food Pantries: https://data.cityofboston.gov/Health/Food-Pantries/vjvb-2kg6
Children Feeding Programs: https://data.cityofboston.gov/Human-Services/Children-s-Feeding-Program/p9yd-36dn
Day Camps: https://data.cityofboston.gov/dataset/Day-Camps/sgf2-btru

To get these datasets: $ python3 getData.py

#Transformations
Transformation 1: The first transformation is to merge the Boston public schools data with the Boston private schools data. This will create a dataset with all of the schools in Boston included. We will focus on the school name and the school's coordinates, while still depicting whether the school is public or private. 

To run the first script: $ python3 mergeSchools.py

Transformation 2: The second transformation is to merge the legacy crime incident reports with the current crime incident reports. This will create a dataset with all of the crime incidents reported from July 2012 to now. We will focus on incident type, date, day of the week, and longitude/latitude.

To run the second script: $ python3 mergeCrimes.py

Transformation 3: The third transformation is to merge the other three datasets that are children dedicated: food pantries, children feeding programs, and day camps. This will create a large dataset detailing different establishments that are dedicated to providing for children. 

To run the third script: $ python3 mergeChildren.py
