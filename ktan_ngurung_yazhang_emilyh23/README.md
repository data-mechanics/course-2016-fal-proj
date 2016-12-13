#Ad-Opt: Find Optimal Advertisement Placement in Boston
### *by Nisa Gurung, Kristel Tan, Yao Zhang, Emily Hou*

# Narrative

Companies spend millions of dollars on advertisements every year. Therefore, it is important to find optimal locations to place them for the most impact on consumers. Eliminating ineffective advertisements can minimize waste while increasing competition and boosting a city’s economy. Accomplishing this not only helps businesses, but also reaps benefits for any given city. In this project, our main focus of research is looking into optimal zip codes in Boston. By looking into physical forms of advertisement around Boston (MBTA bus stops, MBTA T stops, Big Belly garbage locations, college campuses, and Hubway stations), we create an optimization tool in the form of a web service to determine the best locations by zip code in Boston, adjusted to individual need. 

#Datasets

1. [MBTA Bus](https://boston.opendatasoft.com/explore/dataset/mbta-bus-stops/)
2. [T Stops](http://erikdemaine.org/maps/mbta/mbta.yaml)
3. [Big Belly Locations](https://data.cityofboston.gov/City-Services/Big-Belly-Locations/42qi-w8d7)
4. [College/University Locations](https://boston.opendatasoft.com/explore/dataset/colleges-and-universities/)
5. [Hubway Locations](https://boston.opendatasoft.com/explore/dataset/hubway-stations-in-boston/)

#Interactive Web Service & Visualization (description below)
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


#Running Scripts: 

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
In our implementation of algorithms, we performed statistical analysis on potential trends that may exist in the correlation between any two landmark ratings. By interpreting the similarities, if a majority of the comparisons yield a direct/positive correlation, we may choose to either combine the similar datasets or potentially choose one over the other. If such correlations exist, we can analyze whether combining or eliminating landmarks to query would benefit or be detrimental to designing our optimization tool. 

To run and generate the plots:

```
$ python statCorrelation.py

```

![statsCorr](https://github.com/ktango/course-2016-fal-proj/blob/master/ktan_ngurung_yazhang_emilyh23/statCorrelation.png)

Results: There does not seem to be any strong positive or negative correlations among any of two landmark ratings. 


**Optimization**
Preparation:
To find the optimal zip code(s) for physical advertisements based on a rating system, which is from the landmarks outlined in the 5 datasets: MBTA T-stops, MBTA bus stops, Big Belly garbage cans, colleges, and hubway stations. The zip codes will be rated on a 3-star scale: 1-star being the advertising area with the least outreach and 3-star being the area with the most. Using this rating system, we generated an overall numeric rating for each zip code that would be fair after taking into account the number of landmarks in each zipcode and their corresponding square miles. 
`zipcodeRatings.py` will create the finalized overall scores for each zipcode and store it into our database.

To run:

```
$ python zipcodeRatings.py
```

In our optimization, the state space is 2^(the number of zip codes) where 0 represents the choice to not advertise in a certain area and 1 represents the choice to advertise. Depending on the users' query, the objective function, which would be the sum of the permutations of the subset from the original state space that correlates most with the user's query, multipled by population density. We maximized this objective function by returning the zip code with the highest population density. 

In simpler terms, users will be asked to query in the following two ways (in succession):

(1) - the user will be asked to specify the number of optimal zip codes as a result from the query (How many optimal zip codes does the user want to advertise in/is interested in?)

(2) - the user will then query by individual ratings of the 5 individual landmarks

Our web service produces the optimal zip code(s) most similar to the query. 

```
$ python optimize.py
```

**Web Service**
To better visualize, we created a web service that allows users to test out our optimization algorithm:

![webService](https://github.com/ktango/course-2016-fal-proj/blob/master/ktan_ngurung_yazhang_emilyh23/webservice.png)

The web service incorporates Leaflet, an open-source JavaScript library that displays an interactive map. Our map illustrates the zip codes by area, colored by gradients. The gradients are defined by the calculated ratings for each zip code (darker color represents a higher rating and lighter color represents a lower rating). The map is initialized to show one pin in the heart of Boston and shading by overall zip code rating. After users input their queries for the optimal zip code(s), the map will zoom in/out to frame new pin(s) that are dropped on the optimal zip code location area(s). 

On the top right corner of our map, there is a toggle that allows users to select different "views".

"overall" represents overall zip code ratings (initially stored from `zipcodeRating.py`)
"hubway" represents zip code ratings for hubway stations
"tstop" represents zip code ratings for T-stops
"busstop" represents zip code ratings for bus stops
"colleges" represents zip code ratings for college campuses
"bigbelly" represents zip code ratings for Big Belly locations

This is to give users flexibility and different perspectives in analyzing their target audience. 

**Visualization**
For our interactive visualization, we chose to do a zoomable tree map. Tree maps are used to illustrate hierarchical data, which we represent with our rating system. It is implemented in D3.js, allowing for users to view the full dataset of overall rating scores in a compact way (by zooming in and out). 

![treeMap1](https://github.com/ktango/course-2016-fal-proj/blob/master/ktan_ngurung_yazhang_emilyh23/treemap_ratings.jpg)

The sizes of these rating sections are determined by number of zip codes that possess that numeric rating. Clicking on a rating section will result in a zoom that shows the next level in the hierarchy. 

![treeMap2](https://github.com/ktango/course-2016-fal-proj/blob/master/ktan_ngurung_yazhang_emilyh23/treemap_zoomed.jpg)

The sizes of each zip code section are determined by zip code area size and the gradient color of each section is determined by population density. This means that the darker a section is, the more densely populated that zip code is. In this view, we display the median household income for each zip code in the users’ results to provide a better idea for their target audience. 
