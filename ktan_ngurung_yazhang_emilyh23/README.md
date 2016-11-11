# Project #1 Narrative (Question 2a)
### *by Nisa Gurung, Kristel Tan* 

Companies spend millions of dollars on advertisements every year; hence why it is important to place them in locations where the advertisements will have the most impact on consumers. Accomplishing this not only helps businesses, but also reaps benefits for any given city. Eliminating ineffective advertisements can minimize waste while increasing competition for the best ad placements can also help boost a city’s economy. With that said, five publicly available data sets that we will use to help determine the most effective ad placements in the city of Boston include [MBTA Bus](https://boston.opendatasoft.com/explore/dataset/mbta-bus-stops/)and [T Stops](http://erikdemaine.org/maps/mbta/mbta.yaml), [Year 2014 Bluebook 14th Edition](http://www.mbta.com/uploadedfiles/documents/2014%20BLUEBOOK%2014th%20Edition.pdf), [Big Belly Locations](https://data.cityofboston.gov/City-Services/Big-Belly-Locations/42qi-w8d7), [College/University Locations](https://boston.opendatasoft.com/explore/dataset/colleges-and-universities/), and [Hubway Locations](https://boston.opendatasoft.com/explore/dataset/hubway-stations-in-boston/). 

Each of these data sets support our main focus of research in some way by looking at existing physical forms of advertisements around Boston. MBTA Bus and T stops, Big Belly garbage cans, colleges, and hubway stations are all relevant and frequently encountered placements for these ads. We will first attempt to count the number of these landmarks within a given vicinity. Then, we will use information from the Bluebook to identify how densly populated and frequently visited those areas are based on T-stop ridership. 

## Retrieval Script (Question 2b)

The file titled `landmarkLocations.py` will retrieve our intial datasets mentioned above. To run the file:
```
$ python landmarkLocations.py
```

## Transformation Scripts (Question 2c)

We performed three transformations as follows on our datasets.

**1. First Transformation**

This transformation combines T-stop names, locations, and number of entries based on the intersection of stations into a new dataset called tRidershipLocation. It has some limitations due to the Google Maps API quota, but the necessary responses from the API have been stored into a variable to help create the collection. To run the file:

```
$ python tRidershipLocation.py
```
**2. Second Transformation**

This transformation combines colleges and bus stops based on the intersection of their zip codes into a new dataset called collegeBusStopCounts. It has some limitations due to the geocoder query limit, but the necessary responses from the library have been stored into a variable to help create the collection. To run the file:

```
$ python collegeBusStops.py
```

**3. Third Transformation**

This transformation combines hubways and big belly based on the intersection of their zip codes into a new dataset called hubwayBigBellyCounts. It has some limitations due to the geocoder query limit, but the necessary responses from the library have been stored into a variable to help create the collection. This limitation caused us to only sample a subset of the hubway stations dataset. To run the file:

```
$ python hubwayBigBelly.py
```

# Project #2 Narrative
### *by Nisa Gurung, Kristel Tan, Yao Zhang, Emily Hou*

To address the ultimate goal of finding the optimal zip code(s) for physical advertisements based on a rating system, which is from the landmarks outlined in the 5 datasets: MBTA T-stops, MBTA bus stops, Big Belly garbage cans, colleges, and hubway stations. The zip codes will be rated on a 3-star scale: 1-star being the least desirable advertising area and 3-star being the most desirable. To get these final ratings for each zip code in Boston, we merged all datasets (which was taken from the 3 transformation files) on zip code into a dictionary list. For each zip code, we calculated the standard deviation to set the range for each numeric rating (1-3). The same process was applied to generate the overall rating of each zip code into a single numeric value. Potential users will either query ratings for each landmark or query the overall zip code rating, which represents the constraints that will feed into our objective function for later optimization. Our algorithm will generate the optimal result given the user's constraints by focusing on maximizing the viewing population, which we hope would be the most relevant to our analysis. 

In our implementation of algorithms, we plan on performing statistical analysis on potential trends that may exist in the correlation between any two datasets. By interpreting the similarities, if a majority of the comparisons yield a direct/positive correlation, we may choose to either combine the similar datasets or potentially choose one over the other. If such correlations exist, we can analyze whether combinig or eliminating landmarks to query would benefit or be detrimental to designing our final optimization tool. Finally, the bulk of our project is based on our optimization algorithm. In our optimization, the state space is 2^(the number of zip codes) where 0 represents the choice to not advertise in a certain area and 1 represents the choice to advertise. Depending on the users' query, the objective function, which would be the product of population size and the state space, will either be maximized or minimized. 

