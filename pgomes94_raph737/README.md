Patrick Gomes and Raphael Baysa

We decided to look into the optimal area to build a new hospital in the Boston area. The data sets we are using include current hospital locations, police station locations, mbta/train stops, traffic points and crime rates. The new hopsital would be located far from current hospital locations and from police stations, but near high population density locations, preferably near two or more clusters. Accessibility is a concern so it would have to be close to a bus or train stop, but far away from high traffic areas for ambulances. If possible, the optimal hospital would also be located near, but not in, a crime cluster for faster response. 

---
Data Sets
---

Hospitals: https://data.cityofboston.gov/Public-Health/Hospital-Locations/46f7-2snz

MBTA Stops: http://datamechanics.io/data/pgomes94_raph737/stops.csv

Traffic: https://data.cityofboston.gov/dataset/Waze-Point-Data/b38s-xmkq and https://data.cityofboston.gov/Transportation/Waze-Jam-Data/yqgx-2ktq

Crime: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap

Police Stations: https://data.cityofboston.gov/resource/pyxn-r3i2.json

---
Setting up auth.json
---

Only 1 key is required, you can get a key at: https://dev.socrata.com/register

auth.json was formatted:
{
    "services": {
        "cityofbostondataportal": {
            "service": "https://data.cityofboston.gov/",
            "username": "pgomes94_raph737",
            "token": "KPQpbs4UiZMCXzxGYurpLOFwA"
        }
    }
}

---
Getting the data
___

Run dataRequests.py to download all the data into the database.

---
First 2 transformations
---

combineDatasets.py will create 2 more tables in our database, proximity_locations and proximity_cluster_centers.
	proximity_locations: merges 4 of the datasets from before into one table with 2 labels depending on the table it came from.
	proximity_cluster_centers: The kmeans cluster centers for the 2 labels made above. 

---
Last transformation
--- 

This transformation uses pyspark to perform a map function. Installation guidelines for pyspark will be shown below.

The last table created, hospital_scores, is a list of hospital names with their raw rank scores. The higher the score the better the location of the hospital. From here analysis could be done to see how the results fare.

---
pyspark installations
---

I followed this guide, except the last step of setting it up with Jupyter notebook: https://www.dataquest.io/blog/pyspark-installation-guide/

Quickly highlighting the main steps:

1. Make sure any Java version 7+ is installed, otherwise install that.

2.  Go to http://spark.apache.org/downloads.html and Click the link in step 4 to download spark.

3. Download the scala build tool (assumes brew in installed on mac, linux guide in the guide provided above).

4. Add to ~/.bash_profile:
	
	a. export SPARK_HOME="/usr/local/bin/spark-x.x.x" (location where spark was installed)
	
	b. export PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell" (where to start spark)
	
	c. export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH (adds pyspark to pythonpath for imports)
	
	d. export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.10.1-src.zip:$PYTHONPATH (needed because pyspark is built off of spark for Java)
	
	e. export PYSPARK_PYTHON="/LOCATION/OF/PYTHON/VERSION/3.4/bin/python3" (needed if more than one version of python is installed, point to python3)

Once the .bash_profile is updated, completely close the terminal app and reopen to apply these updates to the local environment.
