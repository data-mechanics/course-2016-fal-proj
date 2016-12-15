#Ad-Opt: Find Optimal Advertisement Placement in Boston
### *by Nisa Gurung, Kristel Tan, Yao Zhang, Emily Hou*

## Narrative

In this project, we are looking into optimal zip codes for advertisment placement in Boston. By using five raw datasets (MBTA bus stops, MBTA T stops, Big Belly garbage locations, college campuses, and Hubway stations), we were able to create an optimization tool in the form of a web service to determine the best locations by zip code in Boston, adjusted to individual need. 

##Datasets

1. [MBTA Bus](https://boston.opendatasoft.com/explore/dataset/mbta-bus-stops/)
2. [T Stops](http://erikdemaine.org/maps/mbta/mbta.yaml)
3. [Big Belly Locations](https://data.cityofboston.gov/City-Services/Big-Belly-Locations/42qi-w8d7)
4. [College/University Locations](https://boston.opendatasoft.com/explore/dataset/colleges-and-universities/)
5. [Hubway Locations](https://boston.opendatasoft.com/explore/dataset/hubway-stations-in-boston/)

##Interactive Web Service & Visualization (description below)
Preparation: After complete set up of the MongoDB database, run the retrieval and transformation scripts outlined below.

To run the web service:
Start the MongoDB process and let it run in the background:

```
mongod
```
In a separate window, run the optimization scripts:

```
$ python zipcodeRatings.py

$ python optimize.py
```
Then, run the script for the tree map visualization:

```
$ python treeMap.py
```

Finally, open the local host for the web service:

```
$ python app.py
```
The web page can be found at http://localhost:5000/index.html.
Likewise, the web page for our visualization can be found at http://localhost:5000/visualization.html. This can be reached by the "Visualization" tab.


##Running Scripts: 

**Retrieval**

`landmarkLocations.py` will retrieve our intial raw datasets. 

To run:

```
$ python landmarkLocations.py
```

**Transformations**

`tRidershipLocation.py` combines T-stop names, locations, and number of entries based on the intersection of stations into a new dataset called tRidershipLocation. 

`collegeBusStops.py` combines colleges and bus stops based on the intersection of their zip codes into a new dataset called collegeBusStopCounts. 

`hubwayBigBelly.py` combines hubways and big belly based on the intersection of their zip codes into a new dataset called hubwayBigBellyCounts.

To run:

```
$ python tRidershipLocation.py

$ python collegeBusStops.py

$ python hubwayBigBelly.py
```

**Statistical Analysis**

We performed statistical analysis on potential trends that may exist in the correlation between any two landmark ratings. If any strong correlations exist, we can analyze whether combining or eliminating landmarks to query would benefit or be detrimental to designing our optimization tool. 

To run and generate the plots:

```
$ python statCorrelation.py

```

![statsCorr](https://github.com/ktango/course-2016-fal-proj/blob/master/emilyh23_ktan_ngurung_yazhang/statCorrelation.png)

Results: There does not seem to be any strong positive or negative correlations among any of two landmark ratings. 


**Optimization**

Preparation:

The zip codes are rated on a 3-star scale: 1-star being the advertising area with the least outreach and 3-star being the area with the most. Using this rating system, we generated an overall numeric rating for each zip code that would be fair after taking into account the number of landmarks in each zipcode and their corresponding square miles. 

`zipcodeRatings.py` will create the finalized overall scores for each zipcode and store it into our database.

To run:

```
$ python zipcodeRatings.py
```

(1) - the user will be asked to specify the number of optimal zip codes as a result from the query (How many optimal zip codes does the user want to advertise in/is interested in?)

(2) - the user will then query by individual ratings of the 5 individual landmarks

Our web service produces the optimal zip code(s) most similar to the query. 

```
$ python optimize.py
```

**Web Service**

To better visualize, we created a web service that allows users to test out our optimization algorithm:

![webService](https://github.com/ktango/course-2016-fal-proj/blob/master/emilyh23_ktan_ngurung_yazhang/webservice.png)

The web service incorporates Leaflet, an open-source JavaScript library that displays an interactive map. Our map illustrates the zip codes by area, colored by gradients. A darker color represents a higher rating and lighter color represents a lower rating. After users input their queries for the optimal zip code(s), the map will zoom in/out to frame new pin(s) that are dropped on the optimal zip code location area(s). 

On the top right corner of our map, there is a toggle that allows users to select different "views". It will display zip code ratings that correspond to each type of category (hubway, tstop, busstop, colleges, bigbelly). The map is initialized on "overall", which shows overall zip code ratings, initially stored from `zipcodeRating.py`. This is to give users flexibility and different perspectives in analyzing their target audience. 

**Visualization**

For our interactive visualization, we chose to do a zoomable tree map. It is implemented in D3.js, allowing for users to view the full dataset of overall rating scores in a compact way (by zooming in and out). 

![treeMap1](https://github.com/ktango/course-2016-fal-proj/blob/master/emilyh23_ktan_ngurung_yazhang/treemap_ratings.jpg)

The sizes of these rating sections are determined by number of zip codes that possess that numeric rating. Clicking on a rating section will result in a zoom that shows the next level in the hierarchy. 

![treeMap2](https://github.com/ktango/course-2016-fal-proj/blob/master/emilyh23_ktan_ngurung_yazhang/treemap_zoomed.jpg)

The sizes of each zip code section are determined by zip code area size and the gradient color of each section is determined by population density. In this view, we display the median household income for each zip code to provide a better idea for their target audience. 
