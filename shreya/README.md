#Project 1

My overall goal for this project is to identify trends between average income of neighborhoods and other possibly related factors such as new buildings and crime.

For this Project 1, I chose the following data sets:
- Approved Building Permits (2006-2016)
    - I filtered the dates on this data set so that only building permits approved between July 2012 and August 2015 are in the new dataset, to be consistent with the Crime Incident Reports dataset.
    - The new dataset is a list of a tuples which is in the form (year, list of 45 means found through the k-means algorithm)
- Crime Incident Reports (July 2012 - August 2015)
- Employee Earnings Report 2012
- Employee Earnings Report 2013
- Employee Earnings Report 2014
- Employee Earnings Report 2015

All of the new datasets are of the form: (year, list of 45 means found through the k-means algorithm). This is to see the progression of the building permits, crime, and wealth over the period of 4 years and attempt to find a correlation between the 3.

One problem I haven't yet accounted for is how to account for the fact that the City of Boston employees aren't an accurate representation of all wealth. I was originally going to do this by zipcode, and divide each zipcode's total wealth by the number of people but I'm not sure how to do that with coordinates.
			