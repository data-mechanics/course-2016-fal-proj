For this project we decided to analyze car accidents and other relevant data. Ideally, we want to find regions where car accidents are most likely to happen. Also, we want to find factors corresponding to the number of accidents in a specific area.


## Datasets

1. Car Accidents (http://services.massdot.state.ma.us/crashportal/)
2. Traffic Signals (http://bostonopendata.boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_10)
3. Streetlight Locations (https://data.cityofboston.gov/Facilities/Streetlight-Locations/7hu5-gg2y/data)
4. Transportation Districts (http://bostonopendata.boston.opendata.arcgis.com/datasets/f953f854b672496fb0aa18ad92278f07_5)
5. MBTA Bus Stops (http://bostonopendata.boston.opendata.arcgis.com/datasets/f1a43ad3c46b4ac89b74cdaba393ccac_4)


## Transformations

1. analyze_car_accidents - analyzes car accidents by finding number of streetlights, traffic lights and mbta stops near every accident
2. analyze_mbta_stops - analyzes mbta stops by finding number of streetlights, traffic lights and car accidents near every accident
1. combine_data_by_districts - counts number of traffic signals, streetlights, car accidents and mbta stops in different transportation regions

## Running

First, get all the data buy running all files with prefix "get_"
Then, you can perform 3 transformations by running corresponding files.
