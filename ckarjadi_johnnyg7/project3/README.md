Cody Karjadi and John Gonsalves CS 591 Project Readme

A Characterization of Neighborhood Wealth and New Hospital Location Optimization

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
hospitals (expected positive correlation; wealthier = more hospitals)

Using the statistical results derived from these relationships, we can begin to build a mathematical basis for the wealthiness of a given area. With a better quantification of current areas and their wealth, we can begin to plan better for the future.

**

For this project we demonstrate one way of quantifying how to better distribute a service to a city. Specifically, if we were to build 'n' more hospitals, where should we put them?

We approached this problem as a type of optimization problem; where should we place these 'n' new hospitals so that the weight of traffic jams and crimes are most alleviated from all the currently existing hospitals?

Each crime and jam occurrence was matched with the hospital closest to it. (clustering)

We collected tuples of type (lat,long,num_crimes+num_jams); where lat_long were a location of a hospital.

We calculated the 3D k_means of these tuples, starting with 'n' means (representing the hospitals to be built). 

Finally, the lat,long coordinates of these 'n' hospitals were stored in the database. Using k_means as a type of minimization optimization tool (clustering as evenly as possible with each mean, can be interpreted as "absorbing" the crimes and jams), we can make an informed decision on how to better distribute new hospitals to a city.
**

Execution Instructions: 

Enter mongo environment

mongod -- auth --dbpath "path"
mongo repo -u ckarjadi_johnnyg7 -p ckarjadi_johnnyg7 --authenticationDatabase "repo"

Set Trial to False or True; When True, all execute scripts will only grab some subset of the data


Run retrieve.py; this grabs all the necessary initial data sets.
(e.g. ckarjadi_johnnyg7.collection1,etc)

Run get_avg_execute.py; this calculates the average of the property values for each zipcode

Run correlation_execute.py; this calculates the correlation coeffcients and p values for each collection, and creates a new collection for each one.

(e.g. ckarjadi_johnnyg7.collection_stats -> has a key for correlation coefficient, and p value)

Run hospitals_execute.py; this calculates new hospital locations

**

Visualizations:

Visualizations directory has subdirectories, one for each visualization. Create some localhost: server and you can host the index.html files.
(for windows; python -m http.server ###; when in the directory of the index.html file)

The second visualization uses simple html to show the preexisting hospital locations in Boston, along with the new hospital locations we calculated using k-means. It can be openend and viewed through any web browser.
