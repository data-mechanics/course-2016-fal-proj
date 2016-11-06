//README for jyaang_robinliu106 CS 591 Project 1

We used the following data sets for our project:

Boston Schools: 
https://data.cityofboston.gov/dataset/Boston-Public-Schools-School-Year-2012-2013-/e29s-ympv

Crime Incidence Reports:
https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap

Boston Hospitals:
https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz

Boston Day Camps:
https://data.cityofboston.gov/dataset/Day-Camps/sgf2-btru

Boston School Test Scores:
http://profiles.doe.mass.edu/state_report/mcas.aspx



Project Description:
We will accumulate various metrics on certain characteristics of all the neighborhoods in Boston to calculate a score that measures child-friendliness. We believe that these five characteristics determine child-friendliness: education, safety, parks and recreation, and amenities. To characterize the quality of education in each neighborhood, we will be utilizing data sets on test scores. To measure safety in each neighborhood, we will analyze data sets on local sex offenders and crime incidence reports. As for parks and recreation, we will be using recreational swimming pool and day camp data sets. Lastly, we are looking for data sets on amenities such as nearby hospitals and food inspections. For each neighborhood, we will be computing a score for each characteristic, and taking those scores into consideration all of these scores, we will compute a total score from 1-100.  We intend to make the weight of each factor vary depending on priority. Our working weights that we have now are: 30% education, 30% safety, 25% amenities, and 15% parks/recreation. 


Transformations:
1. Use Map/Reduce to map Boston schools to their respective neighborhoods
2. Use Map/Reduce to map Boston hospitals to their respective neighborhoods
3. Use Map/Reduce to map Boston day camps to their respective neighborhoods
4. Use Map/Reduce to combine Boston schools/day camps to matching neighborhoods
5. Use Map/Reduce to combine Boston hospitals/day camps to matching neighborhoods
6. Use Map/Reduce to combine Boston schools/hospitals to matching neighborhoods
7. Use Map/Reduce to find the average CPI test scores of each school district in Massachusetts.
6. Use Map/Reduce to find match zipcode with district name.


How to Run:

Simply call run.py from terminal



//References http://stackoverflow.com/questions/23643327/mongodb-aggregate-group-array-to-key-sum-value
