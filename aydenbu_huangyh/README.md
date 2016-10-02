# BU CS591L1 Fall 2016 Project 1

**Team Members:**
+ Yehui Huang
+ Xiongying Qiao

##Summary
In this project, we've considered the relationship between the avg income and the number of some kind of public buildings (Hospital, School, HealthStore and Public Garden). The analyze is based on the zipcode.
 
##Datasets
The five datasets we used:
<ol>
<li>'hospital_locations':'https://data.cityofboston.gov/resource/u6fv-m8v4.json'</li>
<li>'employee_earnings_report_2015':'https://data.cityofboston.gov/resource/bejm-5s9g.json'</li>
<li>'health_corner_stores':'https://data.cityofboston.gov/resource/ybm6-m5qd.json'</li>
<li>'community_gardens':'https://data.cityofboston.gov/resource/rdqf-ter7.json'</li>
<li>'public_schools':'https://data.cityofboston.gov/resource/492y-i77g.json'</li>
</ol>

##Porcess
Firstly we can obtain the number of average earnings in each zipcode area by using MapReduce to project and reduce to get the total earnings and counts in each zipcode, then we can obtain the average earning by (total earning / counts) for each zipcode area.
