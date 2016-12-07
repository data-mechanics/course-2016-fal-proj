I chose to work with these five data sets:

* Crime Incident Reports
 * https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx
 * https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-by-Weapon-Type/vwgc-k7be
* Boston Police Department Firearms Recovery Counts
 * https://data.cityofboston.gov/Public-Safety/Boston-Police-Department-Firearms-Recovery-Counts/vb7h-wnyg
* Hospital Locations
 * https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz
* Police Departments Map
 * http://bostonopendata.boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6

#### Project Description

Our plan is to find if a correlation exists between the number of firearms recovered by the Boston Police Department and the number of crimes involving firearms in Boston. We expect that the number of crimes involving firearms should decrease after a greater number of firearms have been recovered. We want to analyze how effective the recovery of firearms is in decreasing crimes with firearms. The Firearm Recovery Counts data set contains data for about a one year period from 8/20/2014 to 7/27/2015, therefore we only use the crime data from this same time period. We also use police department location data to determine in which areas of Boston firearm crimes are most frequent.

I performed transformations on Crime Incident Reports, Firearm Recovery Counts, and Police Departments to generate three new data sets.

#### Transformations

1. I retrieve crime data and use the crimes involving firearms. I only use crimes that occurred between 8/20/14 and 7/27/15 since the firearm recovery data was only available for this time period.

2. I retrieve Boston police departments data and merge this with crimes data to create a new data set. This data set contains a list of each district and the total number of firearm crimes that occurred in that district during this time period. I also included location data for each police department which can be used to create a visualization of which areas in Boston are most affected by firearm crimes.

3. I retrieve firearm recovery data and merge this with crimes data to create a new data set. I aggregated the number of crimes that occurred on each day during this time period. I also added the three fields for recovered firearms to create a new field with the total number of firearms recovered on each day. The final data set contains an entry for each date with the total number of crimes, the total number of firearms recovered, and a list of IDs for each crime that occurred on that day.

I used the default format for my auth.json file:
```
{
	"services": {
		"cityofbostondataportal": {
			"service": "https://data.cityofboston.gov/",
			"username": "myusername",
			"token": "mytoken",
			"key": "mykey"
		}
	}
}
```

#### Firearm Recovery and Crime Analysis

Before performing this analysis we believed there should be a negative correlation between the number of firearms recovered and the number of firearm-related crimes. We predicted that as the number of firearms recovered increases, the number of crimes should decrease. We found a correlation coefficient of 0.068 and a p-value of 0.29. This correlation coefficient indicates a very weak positive correlation between the data sets. However, the p-value is high so we cannot reject the null hypothesis.

A limitation of our analysis is the limited amount of available data to test this correlation. We could more accurately perform this analysis if we had firearm recovery data for a longer time period.

#### Police Station Location and Firearm Crime Rate Analysis

For this analysis, we used the District Crime police station coordinates and the coordinates of where crimes occurred from the Crime dataset. In the District Crime dataset, there are 12 police stations; however, A1 and A15 police stations service the same area so we set our K means to 11 distinct police stations.  For this problem, we wanted to analyze whether the current locations of the police stations are optimal against the locations of the most firearm crimes.

The locations of the current police stations are:
[(42.36182537306611, -71.06030835115676), (42.37107828877771, -71.03862257414728),
 (42.32826223235851, -71.0841094400415), (42.284701745777966, -71.09173715900582),
 (42.340803314800766, -71.05413848836878), (42.29807150326232, -71.05916344668708),
 (42.33944800951563, -71.06919575510932), (42.34940243310216, -71.15055032983688),
 (42.28678520305471, -71.14835428921228), (42.30971862463083, -71.10463273773469),
 (42.25648289119162, -71.12426985783104)]

 The locations of where the police stations should be based on the K means algorithm:
 [(42.25667682352941, -71.12133337254902), (42.32554700294984, -71.08511305604719),
 (42.376740450980385, -71.03555701960785), (42.28568958620691, -71.08677169950738),
 (42.30006430496453, -71.06718122340425), (42.334550428571426, -71.05053597619046),
 (42.36497875675675, -71.06099356756756), (42.33857692592592, -71.07255938888886),
 (42.351098131578944, -71.13942431578946), (42.28331083333333, -71.13512027777777),
 (42.31425064666666, -71.09992165999998)]

 The absolute difference between the two datasets are:
 [(0.10514854953670039, 0.061025021392254075), (0.04553128582787025, 0.04649048189990879),
  (0.04847821862187374, 0.04855242043365138), (0.000987840428940956, 0.004965459498436076),
  (0.04073900983623702, 0.013042735035469377), (0.03647892530910468, 0.008627470496620049),
  (0.025530747241120366, 0.00820218754175528), (0.010825507176242866, 0.07799094094801262),
  (0.06431292852423098, 0.008929973422823423), (0.02640779129750115, 0.030487540043083072),
  (0.05776775547504087, 0.024348197831059792)]

  There is only a small difference between the current locations of the police stations and the k means calculated locations.  From this data, we can conclude that the police stations are in a good location relative to where crimes that involved firearms occurred.

  For the trial part of this assignment, we did not incorporate the trial settings on the district crime data since there is only 12 districts in total. 

#### Visualizations

1.  index.html uses d3.js and a geoJSON file created from the coordinates of our dataset.  It shows an overlay of Boston, in which the borders are the police districts.  Each crime that was registered with a coordinate point has been placed on the map, showing the crime density.

#### Conclusion

We found no significant correlation between the number of firearms recovered by Boston police departments and the number of crimes involving firearms. However, this analysis is constrained by the limited amount of available firearm recovery data. Using the k-means algorithm, we found that police departments in Boston are ideally located at the center of areas with a high concentration of firearm crimes.

