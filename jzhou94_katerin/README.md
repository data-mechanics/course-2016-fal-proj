Narrative
---------

The five datasets are employee earnings in Boston, location of public schools in Boston, crime incidents in Boston, colleges and universities in Boston and firearms recovery counts in Boston. These datasets can be combined on location to see various statistics relating to crime in different counties of Greater Boston area. Specifically, we want to see the presences of schools on the occurrences and different crimes. Also, we want to see if there is a correlation between wage of police officers and general crime in an area. These questions are interesting because politicians often point to increasing security and education as ways to improve living conditions.

We also added street light locations in Boston as a 6th dataset and decided not to use the firearms data as we believed no useful information could be derived from the dataset.

* https://data.cityofboston.gov/resource/bejm-5s9g.json employee earnings
 * https://dev.socrata.com/foundry/data.cityofboston.gov/bejm-5s9g employee earnings
* https://data.cityofboston.gov/resource/492y-i77g.json public schools
 * https://dev.socrata.com/foundry/data.cityofboston.gov/492y-i77g public schools
* https://data.cityofboston.gov/resource/ufcx-3fdn.json crime incident
 * https://dev.socrata.com/foundry/data.cityofboston.gov/ufcx-3fdn crime incident
* https://www.opendatanetwork.com/dataset/data.cityofboston.gov/vb7h-wnyg firearms
 * https://data.cityofboston.gov/api/views/vb7h-wnyg/rows.json?accessType=DOWNLOAD firearms
* http://bostonopendata.boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2?uiTab=table   Colleges
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

Web APIs:

*Scatterplot: Using flask to extract crime and school data from mongodb, we were able to display the data on html. We then used the data in a scatter plot to see if there's a correlation between the number of crimes to schools for the zip codes in Boston.

*Map:
Using a mapping API, we attempted to display the average salary of public worker to crime rate for each zip code on a map. The interaction would have been the choice of type of public worker, from police officer, firemen, patrol officers, to compare to crime rates. However, we were not able to successfully integrate the mapping API with our data.

For a full summary of the report, open index.html.