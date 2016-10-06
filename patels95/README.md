I chose to work with these five data sets:

Crime Incident Reports
https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx
https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-by-Weapon-Type/vwgc-k7be

Boston Police Department Firearms Recovery Counts ***
https://data.cityofboston.gov/Public-Safety/Boston-Police-Department-Firearms-Recovery-Counts/vb7h-wnyg

Hospital Locations
https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz

Police Departments Map
http://bostonopendata.boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6

My plan is to find if a correlation exists between the number of firearms recovered by the Boston Police Department and the number of crimes involving firearms in Boston. My expectation is that the number of crimes involving firearms should decrease after a greater number of firearms have been recovered. I want to analyze how effective the recovery of firearms is in decreasing crimes with firearms. The Firearm Recovery Counts data set contains data for about a one year period from 8/20/2014 to 7/27/2015, therefore I only use the crime data from this same time period. I will also use police departments data to determine in which areas of Boston firearm crimes are most frequent.

I performed transformations on Crime Incident Reports, Firearm Recovery Counts, and Police Departments to generate three new data sets.

Transformations:

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
