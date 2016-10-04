# course-2016-fal-proj
Project repository for the course project in the Fall 2016 iteration of the Data Mechanics course at Boston University.

# Datasets Used:
We chose to use the following public dataset from the Boston Data Portal: 311, Service Request (which we'll use to outline fire incidents happening in Boston). We will also be using the public datasets from the Boston Maps Open Data: Fire Districts (outlines the zones of fire districts), Fire Departments (outlines the locations of fire departments around in Boston), Fire Hydrants (outlines the location of fire hydrants around Boston), and Fire Boxes (outlines the location of fire boxes around Boston). 

311, Service Requests: https://data.cityofboston.gov/City-Services/311-Service-Requests/awu8-dc52
Fire Districts: http://bostonopendata.boston.opendata.arcgis.com/datasets/bffebec4fa844e84917e0f13937ec0d7_3
Fire Departments: http://bostonopendata.boston.opendata.arcgis.com/datasets/092857c15cbb49e8b214ca5e228317a1_2
Fire Hydrants: http://bostonopendata.boston.opendata.arcgis.com/datasets/1b0717d5b4654882ae36adc4a20fd64b_0
Fire Boxes: http://bostonopendata.boston.opendata.arcgis.com/datasets/3a0f4db1e63a4a98a456fdb71dc37a81_4

After obtaining this information, we'll analyze how well fire incidents are being attended to around different parts of Boston by district. We would like to know if there are any areas that require more fire attention than others and if further safety actions need to be taken. 

# Retrieving Data

Getting data from all of the datasets and importing them into the repo database by running data.py. 

$python data.py

# Transforming Data
Transformation #1: Run fireCounts.py to create a new dataset by mapping and reducing the 311 requests by district, which returns the number of incidents in each fire district for "Fire".

Transformation #2: Run fireDepCounts.py to create a new dataset by mapping and reducing the 311 requests by district, which returns the number of incidents in each fire district for "Fire Department".

Transformation #3: Run hydrantCounts.py to create a new dataset by mapping and reducing the 311 requests by district, which returns the number of incidents in each fire district for "Fire Hydrant".
