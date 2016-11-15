# Assignment #2
## Narrative
In the previous assignment, we generated a data set mapping each crime to the number of streetlights within a given radius. We will be using that information to calculate the number of crimes that happen within a radius of x number of streetlights. Our goal is to create a linear regression between the number of streetlights and the number of crimes, through which we may be able to determine a relationship between the two. We are using the _Pearson product-moment correlation coefficient_ to measure the linear dependence between our two variables, and we predict to see an overall negative linear correlation. I.e. the more streetlights in an area, the less crimes happen.

To further our goal of seeing which factors influence crime, we are using our previous mapping of crimes to the average property value in a radius surrounding that crime. We are expanding this to determine the number of crimes that happen within a given range of property values. This will allow us to see any relationship between the two, and what the linear correlation may be. 

Together, these two will provide further insight into which factors influence crime, and can in turn be used to predict which areas are more prone to criminal activity. 

# Assignment #1
## Project
The purpose of this project is to figure out which factors influence crime. With the information we produce, we can predict new potential hotspots for crimes and what measurements can be taken to limit the problem.

## Data Sets
* [Crime Incident Reports](https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-August-2015-To-Date-Source-/fqn4-4qap)
* [Streetlight Locations](https://data.cityofboston.gov/Facilities/Streetlight-Locations/7hu5-gg2y)
* [Police District Stations](https://data.cityofboston.gov/Public-Safety/Boston-Police-District-Stations/23yb-cufe)
* [Food Pantries](https://data.cityofboston.gov/Health/Food-Pantries/vjvb-2kg6)
* [Property Assessment](https://data.cityofboston.gov/Permitting/Property-Assessment-2016/i7w8-ure5)

## Transformations
Streetlights will be combined with crime to generate an overview of how many streetlights within a certain radius around a crime location.
Police Stations will also be combined with crime to figure out the distance from a crime to the closest police station.
Lastly, we've combined Property Assessments with crime to calculate the average property value in an area surrounding the crime.

## Dependencies
* [Pandas](https://pypi.python.org/pypi/pandas/0.18.1/): Data manipulation library
* [Numpy](https://pypi.python.org/pypi/numpy): Scientific computing library

### Installation
```shell
pip install pandas
```

## Running the project
```shell
python main_file.py
```