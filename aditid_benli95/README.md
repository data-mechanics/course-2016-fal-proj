Project#1
======

##Narrative
Say for instance, every morning at rush hour there's a huge series of traffic jams created by people all trying to access drive into the city and find available parking for their cars. You're a municipal worker who wants to fix this problem.  We've created three new data sets from working with previous  data sets online that might be helpful for this task.  

The very first new data set (produced by `transformation0.py`) is a dataset that aggregates all the high parking demand related traffic tickets to the nearest commercial parking lot.  Based on the amount of illegal parking in an area of each commercial parking lot, you can infer what parking areas are over-utilized or under-utilized.  Using this information you could expand parking facilities in certain areas or shut down some in others to create a more balanced distribution of traffic. 

The second dataset (produced by `transformation1.py`) takes the street name as the key and aggregates all jams and crime incidences for each street. We are hoping to see if perception of safety plays a part in preferred routes of drivers. Latitude and longitude have also been added to the streets allowing us to compare future datasets based on this measure.

The third dataset (produced by `transformation2.py`) is one that maps the total number of intersections to a particular road, given that intersections greatly increase the degree of traffic on a road.  Using this information, municipal workers can attempt to plot parking locations in such a way that common routes to them do not pass through a great deal of intersection heavy roads.

##Publicly Available Datasets Used
1. [Waze Jam Data](https://data.cityofboston.gov/Transportation/Waze-Jam-Data/yqgx-2ktq/data)
2. [Commercial Parking Structures](https://data.cambridgema.gov/Traffic-Parking-and-Transportation/Commercial-Parking/t8tm-muns)
3. [Intersections](https://data.cambridgema.gov/Traffic-Parking-and-Transportation/Intersections/8m9a-yuzk)
4. [Metered Parking](https://data.cambridgema.gov/Traffic-Parking-and-Transportation/Metered-Parking-Spaces/6h7q-rwhf)
5. [Parking Tickets](https://data.cambridgema.gov/Traffic-Parking-and-Transportation/Cambridge-Parking-Tickets-for-the-period-January-2/vnxa-cuyr)
6. [Crime Incident Reports](https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap)

##Three New Datasets
1. LotsWithAdjacentTickets
2. jamCrime
3. jamInters

*Other intermediary datasets may be ignored*

***

##Files Submitted and Instructions

####README.md
* documentation and justification explaining how these data sets can be combined to answer an interesting question or solve a problem

####retrieveDataSets.py
* produces publicly available datasets

####transformation0.py
* produces LotsWithAdjacentTickets

####transformation0.py
* produces jamCrime

####transformation0.py
* produces jamInters

