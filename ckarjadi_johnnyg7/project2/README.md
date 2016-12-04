Cody Karjadi and John Gonsalves Project 2 Readme



Narrative:

How do we better distribute services to less wealthy areas of a city?

To answer this question, we must first quantify the wealthiness of an area, and define what an 'area' is.

For this project, we defined areas by zip codes, and quantifed wealthiness by mean gross taxes of properties in a given area.

For example, a given zip code may have a mean gross property tax of $100,000.

Then, for a given factor, we can calcuate the correlation coefficient and p value in a given area.

e.g. for crimes:

for every zip code we created a tuple -> (avg_prop_val,# crimes); and then calculated the correlation coefficient and p values.

We considered the following factors:

food pantries (expected negative correlation; wealthier = less pantries)
fast food (expected negative correlation; wealthier = less fast food)
community gardens (expected positive correlation; wealthier = more community gardens)
crime (expected very negative correlation; wealthier = less crimes)


Using the statistical results derived from these relationships, we can begin to build a mathematical basis for the wealthiness of a given area. With a better quantification of current areas and their wealth, we can begin to plan better for the future.

**

For this project we demonstrate one way of quantifying how to better distribute a service to a city. Specifically, if we were to build 'n' more hospitals, where should we put them?

We approached this problem as a type of optimization problem; where should we place these 'n' new hospitals so that the weight of traffic jams and crimes are most alleviated from all the currently existing hospitals?

Each crime and jam occurrence was matched with the hospital closest to it.

We collected tuples of type (lat,long,num_crimes+num_jams); where lat_long were a location of a hospital.

We calculated the 3D k_means of these tuples, starting with 'n' means (representing the hospitals to be built). 

Finally, the lat,long coordinates of these 'n' hospitals were stored in the database. Using k_means as a type of minimization optimization tool (clustering as evenly as possible with each mean, can be interpreted as "absorbing" the crimes and jams), we can make an informed decision on how to better distribute new hospitals to a city.
**

Project 2:
Enter mongo environment

mongod -- auth --dbpath "path"
mongo repo -u ckarjadi_johnnyg7 -p ckarjadi_johnnyg7 --authenticationDatabase "repo"

Run retrieve.py; this grabs all the necessary initial data sets.
(e.g. ckarjadi_johnnyg7.collection1,etc)

Run get_avg_execute.py; this calculates the average of the property values for each zipcode

Run correlation_execute.py; this calculates the correlation coeffcients and p values for each collection, and creates a new collection for each one.

(e.g. ckarjadi_johnnyg7.collection_stats -> has a key for correlation coefficient, and p value)

Run hospitals_execute.py; this calculates where the 'n' number of hospitals should be built in order to "absorb" the crime and jam occurrences.

Correlation Drawbacks:

There may be 'wasted data'. If we only have some 10 zip code values for avg property values, for a given factor, that given factor may have 20 zip code values - or just 10 different zip code values. This is an unfortunate drawback of the nature of the data we have access to from the socrata api / data.cityofboston.gov. It is mitigated by taking a very large sample size (150,000+), but it is a non-trivial consideration.

Possible solution: perhaps to avoid this wasted/mismatched data, the paradigm could use lat/long and cluster by closest distance instead of zip codes, or attempt to create/find some new metric that may avoid wasted data, without creating large inaccuracies.

Trial Mode:
If True in retrieve.py; only grabs parts of the relevant datasets. (e.g. limit = 1000)

If False in retrieve.py; grabs all of the datasets. (there are still limits in the query call, because the socrata api seems to only retrieve 1000 data entries by default. this is circumvented by putting a "limit" higher than 1000, to access more than 1000 data entries.)

If false in hospital_execute, get_avg_execute and correlation_execute, acts the same way as if trial was true. We did not think that trial mode would have any effect on these two files. They don't make any socrata api calls, they simply call on the data collections that have been *retrieved*. 

If trial mode wants to be executed on the hospital and correlation execute files, it only needs to be set to *True for retrieve.py*.