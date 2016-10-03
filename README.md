# Project #1 Narrative (Question 2a)
### *by Nisa Gurung and Kristel Tan* 

Companies spend millions of dollars on advertisements every year; hence why it is important to place them in locations where the advertisements will have the most impact on consumers. Accomplishing this not only helps businesses, but also reaps benefits for any given city. Eliminating ineffective advertisements can minimize waste while increasing competition for the best ad placements can also help boost a city’s economy. With that said, five publicly available data sets that we will use to help determine the most effective ad placements in the city of Boston include [MBTA Bus](https://boston.opendatasoft.com/explore/dataset/mbta-bus-stops/)and [T Stops](http://erikdemaine.org/maps/mbta/mbta.yaml), [Year 2014 Bluebook 14th Edition](http://www.mbta.com/uploadedfiles/documents/2014%20BLUEBOOK%2014th%20Edition.pdf), [Big Belly Locations](https://data.cityofboston.gov/City-Services/Big-Belly-Locations/42qi-w8d7), [College/University Locations](https://boston.opendatasoft.com/explore/dataset/colleges-and-universities/), and [Hubway Locations](https://boston.opendatasoft.com/explore/dataset/hubway-stations-in-boston/). 

Each of these data sets support our main focus of research in some way by looking at existing physical forms of advertisements around Boston. MBTA Bus and T stops, Big Belly garbage cans, colleges, and hubway stations are all relevant and frequently encountered placements for these ads. We will first attempt to count the number of these landmarks within a given vicinity. Then, we will use information from the Bluebook to identify how densly populated and frequently visited those areas are based on T-stop ridership. 

## Retrieval Script (Question 2b)

The file titled `landmarkLocations.py` will retrieve our intial datasets mentioned above. To run the file:
```
>>> python landmarkLocations.py
```

## Transformation Scripts (Question 2c)

We performed three transformations as follows on our datasets.

1. Transformation 1

...This transformation T-stop names, locations, and number of entries into a new collection called ridershipLocation. 
...To run the file:

```
>>> python tRidershipLocation.py
```

