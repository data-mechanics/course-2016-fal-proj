Narrative
---------

The five datasets are employee earnings in Boston, location of public schools in Boston, crime incidents in Boston, colleges and universities in Boston and firearms recovery counts in Boston. These datasets can be combined on location to see various statistics relating to crime in different counties of Greater Boston area. Specifically, we want to see the presences of schools on the occurrences and different crimes. Also, we want to see if there is a correlation between wage of police officers and general crime in an area. These questions are interesting because politicians often point to increasing security and education as ways to improve living conditions.

* https://data.cityofboston.gov/resource/bejm-5s9g.json employee earnings
 * https://dev.socrata.com/foundry/data.cityofboston.gov/bejm-5s9g employee earnings
* https://data.cityofboston.gov/resource/492y-i77g.json public schools
 * https://dev.socrata.com/foundry/data.cityofboston.gov/492y-i77g public schools
* https://data.cityofboston.gov/resource/ufcx-3fdn.json crime incident
 * https://dev.socrata.com/foundry/data.cityofboston.gov/ufcx-3fdn crime incident
* https://www.opendatanetwork.com/dataset/data.cityofboston.gov/vb7h-wnyg firearms
 * https://data.cityofboston.gov/api/views/vb7h-wnyg/rows.json?accessType=DOWNLOAD firearms
* http://bostonopendata.boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2?uiTab=table   public schools
Transformations
---------

* Mapreduce Employee Earnings: Uses mapreduce to calculate average city of Boston Employee earnings for each zip code
* Mapreduce Schools: Uses mapreduce to calculate the number of schools located in a zip code
* Mapreduce Crime: Uses mapreduce to project police districts into zip codes and calculate number of crimes per zipcode
* Merge: Uses list comprehension to combine the number of schools in each district and the number of crimes.

Statictical Analyses
---------
* p-value:

    Null Hypothehis: average earnings of police officers HAS NO effect on the number of crimes. Therefore, the average           number of crimes should be the same in every district.

    Alternative Hypothehis: average earnings of police officers HAS an effect on the number of crimes. Therefore, the           average number of crimes should be negatevily correlated with the average earnings of police officers.

    After executeing the p-value algorithm, we got a p-value equal to 1. Therefore, we cannot conclude that a significant       difference exists between null and alternative hypotheses. 

* k-means:
    We used k-means algorithm in order to find the point with the highest density of crimes. 
