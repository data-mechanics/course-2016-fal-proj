# How streetlights deter crime

Boston University Department of Computer Science

CS591 L1 Fall 2016 

Professor Andrei Lapets

By [Ying Hang Eng](mailto:yinghang@bu.edu) and [Aleksander Skjoelsvik](mailto:alsk@bu.edu)

## Introduction
For our project we have attempted to figure out which factors influence crime, and if successful, what direct measures can be taken to limit the problem. 

Through our use of datasets provided by The City of Boston, we have been able to map crimes to a number of other relevant traits, and from that deduct if there is a correlation. This can be used to pinpoint locations exposed to crime that would benefit from certain improvements.

## Datasets
Our core dataset is the **[Crime Incident Reports](https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap)**. This is provided by The Boston Police Department through The City of Boston data portal and contain all official crime reports from August 2015 to current day. This dataset laid the groundwork for which all our other research is built on.

The second most important dataset we ended up working with is the **[Streetlight Locations](https://data.cityofboston.gov/Facilities/Streetlight-Locations/7hu5-gg2y)** dataset provided by The Department of Public Works through The City of Boston data portal, which contains the coordinate locations of every current streetlight in Boston. The full map of all streetlights, as provided by The City of Boston data portal, is shown below.

![Streetlight Data](streetlight_data.png)

To further our research, we also used the **[Police District Stations](https://data.cityofboston.gov/Public-Safety/Boston-Police-District-Stations/23yb-cufe)** dataset. This contains the coordinate location of all Police District Stations in Boston. 

In addition, we also looked at the **[Food Pantries](https://data.cityofboston.gov/Health/Food-Pantries/vjvb-2kg6)** dataset. It contains the coordinate location of every registered food pantry throughout the city, along with some other non-relevant information.

Finally, we worked with the **[Property Assessment](https://data.cityofboston.gov/Permitting/Property-Assessment-2016/i7w8-ure5)** dataset. The information provided is very extensive, but we ended up relying mainly on the total assessed property value. 

## Transformations
The first step towards our goal was to combine the crime dataset with a number of the other datasets we had retreived to see if we could find a correlation between them. 

A transformation that mapped each crime to the number of streetlights within a certain radius supported our suspicion that there may be an association between them. 

We also combined the property assessment data set with crime to calculate the average property value in a radius surrounding each crime, which also furthered our suspicion that it may be relevant. 

Finally we decided to calculate the distance from every crime to the closest police district station. Sadly this did not seem to yield the same results as the other two, and we therefore decided not to pursue this further. 

Once we had the relevant transformations, we moved onto finding an actual correlation between them. We used the new crime-streetlight dataset to calculate the number of crimes that happen within a radius of _20 meters_ of x number of streetlights. Then we created a linear regression between the number of streetlights and the number of crimes, using the _Pearson product-moment correlation coefficient_ to measure the linear dependence between our two variables. 

> __Pearson product-moment correlation coefficient__:
> `ρ(x, y) = cov(x, y) / (σ(x) ⋅ σ(y))`

[Source](http://cs-people.bu.edu/lapets/591/s.php#2a91b48a2e1040808d9538ff45aede2f)

We also used our previous mapping of crimes to the average property value to determine the number of crimes that happen within a given range of property values. 

## Results
Due to the poor quality of the property value dataset, we were not able to confidently confgirm a relationship between the average property value in an area and the number of crimes. 

However, the crime-streetlight mapping gave us an overall linear correlation of __-0.85__, which proves there is a relationship between the two. More specifically, the more streetlights in an area the less crimes happen. 

Below is the raw data mapping the number of crimes that happen within vincinity to a number of streetlights. Though there are some outliers, the overall trend shows a negative correlation (as confirmed by our the product-moment coefficient). The large outlier at 2 streetlights is due to the fact that there are far more areas in close vincinity to 2 streetlight than areas with only 1 or 0. 

![Crimes within radius of 20 meters of x number of streetlights](crime_streetlight_data.png)

Because we succesfully confirmed our suspicion about how streetlights deter crime using actual data from The City of Boston, we can now look at how to use this information to make actual real-world optimizations in terms of limiting crime. This will be discussed further in the 'Visualizations' section.

### Visualizations
To make this more clear, we decided to visualize the latter two parts our project. Namely the relationship between crimes and streetlights, and the optimizations that can be made.

For the first we used a bar chart that depicts the negative correlation between number of streetligths and number of crimes. This confirms the pearson product-moment correlation coefficient we calculated earlier, by indicating the higher number of streetlights the less crimes. Due to the sheer scale of the numbers, crimes with more than 10 streetlights are not shown on the graph. Please refer to the raw data above.  

![Crime-Streetlight Bar Chart](crime_streetlight_barchart.png)

To more easily make suggestions in terms of limiting crime, it would be beneficial to visually map our data. Through the use of a geographic heatmap that depicts areas with more streetlights as warmer, with each crime plotted as a point, we can easily see which areas are currently exposed to crime. If this area also coincide with less streetlights, we can conclude that this area could benefit from more streetlights. Below is a screenshot of our mapped visualization, where one can easily see the relationship between poorly-lit areas and more crime. 

![Crime-Streetlight Heatmap Zoomed Out](crime_streetlight_heatmap_min_zoom.png)

Referring to the image above, one can see that there are 873 crimes in the Downtown/ Chinatown area alone, an area that according to our data is less under-lit compared to other parts of Boston (such as Beacon Hill). Though there are far more factors at play than just streetlights, this could be the starting point for more in-depth research. The image below is a zoomed-in version of the Chinatown area from the map above, and though it can be hard to make out at first glance, there are far more crimes happening (ref yellow circles) in areas with less streetlights (ref lighter blue areas). 

![Crime-Streetlight Heatmap Zoomed In](crime_streetlight_heatmap_max_zoom.png)

## Conclusion
Through the use of data provided by The City of Boston, we have been able to determine a negative correlation between crimes and number of streetlights. Using our transformations and visualization, we can also make suggestions for which areas could benefit from more streetlights. Though there are more factors that contribute to crime, this could help give officials an overview of which areas are in need of more thorough research, and possibly more streetlights if another strong contributor cannot be found.

### Future work
We have already made solid progress towards our goal of determining which factors influence crime and how to limit the problem. Building on this, it would be very interesting to determine other relevant factors and use them in combination with what we have to limit crime. This could be for example extending our current transformations and algorithms to work with other quality data sets. There were a number of data sets we did not pursue due to the inconsistency in data, such as property values, but we would love to explore this further if time permitted.  

## Dependencies
* [Pandas](https://pypi.python.org/pypi/pandas/0.18.1/): Data manipulation library
* [Numpy](https://pypi.python.org/pypi/numpy): Scientific computing library
* [Leaflet.js](http://leafletjs.com): JavaScript map library
* [Node.js](https://nodejs.org/en/): JavaScript runtime library
* [Express](http://expressjs.com): Node.js web framework
* [Mongoose](http://mongoosejs.com): Node.js MongoDB object modeling library

### Running the transformations
```shell
python main_file.py
```

### Running the web application

First, you need to install the dependencies

```shell
npm install
```
Then, edit line 4 in web_app/models/models.js to the appropriate mongo database uri

```javascript
var connect = 'mongodb://admin:example@localhost:27017/repo';
```

Lastly, start the server and visit http://localhost:3000

```shell
npm start
```

### Limitations & potential issues

- Visualization #2 is done in a way where the browser is actually making API calls using jQuery/AJAX to the backend to obtain the data points and then plotting them out. However, due to the huge size of the datasets (150k+ items), it usually takes the browser a while to handle the data and sometimes it might even freezes. Because of that, we're currently only requesting 80k+ datapoints and that can be changed at your own discretion in the **web_app/views/index.jade** file, line 59 & 75

  ```shell
  url: 'http://localhost:3000/crimes/40000'
  ```

### Web Stack

- Front-end: jQuery + D3 + Leaflet + MapBox
- Back-end: Node.js + Express
- Visualization #1 - D3.js + data file
- Visualization #2 - Making REST-ful calls to backend and plotting it out

