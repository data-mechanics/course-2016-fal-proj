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

# Project #2 Narrative (Question 2a)
### *by Nisa Gurung, Kristel Tan, Yao Zhang, Emily Hou*

To address the ultimate goal of finding the optimal zip code(s) for physical advertisements based on a rating system, which is from the landmarks outlined in the 5 datasets: MBTA T-stops, MBTA bus stops, Big Belly garbage cans, colleges, and hubway stations. The zip codes will be rated on a 3-star scale: 1-star being the least desirable advertising area and 3-star being the most desirable. To get these final ratings for each zip code in Boston, we merged all datasets (which was taken from the 3 transformation files) on zip code into a dictionary list of calculated ratios between total counts of each landmark and corresponding square miles of that zip code. We then calculated the standard deviation to define the range for the rating system. In this way, we can determine a numeric rating for each zip code based off of this range. The same process was applied to generate the overall rating of each zip code into a single numeric value. Potential users will either query ratings for each landmark or query the overall zip code rating, which represents the constraints that will feed into our objective function for later optimization. Our algorithm will generate the optimal result given the user's constraints by focusing on maximizing the viewing population, which we hope would be the most relevant to our analysis. 

```
$ python zipcodeRatings.py

```

## Statistical Analysis (Question 2b)
In our implementation of algorithms, we plan on performing statistical analysis on potential trends that may exist in the correlation between any two landmark ratings. By interpreting the similarities, if a majority of the comparisons yield a direct/positive correlation, we may choose to either combine the similar datasets or potentially choose one over the other. If such correlations exist, we can analyze whether combinig or eliminating landmarks to query would benefit or be detrimental to designing our final optimization tool. 
Results: We plotted the ratings for all the landmarks to see if there was any correlations. The correlation coefficients are printed out to show that there are no strong position or negative correlations among any of two landmark ratings. 

```
$ python statCorrelation.py

statCorrelation.png
```

## Optimization (Question 2b)
Finally, the bulk of our project is based on our optimization algorithm. In our optimization, the state space is 2^(the number of zip codes) where 0 represents the choice to not advertise in a certain area and 1 represents the choice to advertise. Depending on the users' query, the objective function, which would be the sum of the permutations of the subset from the original state space that correlates most with the user's query multipled by population density. We plan on maximizing this objective function. In simpler terms, users will be asked to query in three different ways:
(1) - the user can query by optimal overall rating of a zip code, which our tool would produce a single zip code with the highest population density for that particular rating. 
(2) - the user can query by individual ratings of the zip code (by ratings of the 5 individual landmarks), which our tool would produce the optimal zip code most similar to the query with the highest population density. 
(3) - the user can query by a specific number of optimal zip codes that follow the same logic as in (2). 
Additionally, we scraped the median household income for each zip code to give the user a better idea of their target audience. In the same way, we also determined the corresponding ratings for these income values, which our tool will display in accordance to the resulting zip code. 

```
$ python optimize.py
```

## Trial Mode (Question 3b)
To test on trial mode, set the `trial` parameter within `zipcodeRatings.py` of the `execute()` method to `True`. It will activate the capability defined within the if-else statements when data is first pulled from the database to be created as dictionaries. 
