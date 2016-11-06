Project
-------

I chose to work with the crime, 311 request, mayor 24hr hotline, potholes and Department of Neighborhood Development properties datasets. The idea is to the Broken Windows Theory which states that maintaning and monitoring urban environments to prevent small crimes will prevent more serious crimes from happening.

`collectData.py`: To run type: `python3 collectData.py`. This scripts collects the data from the City of Boston data portal.

`dataTransform1.py`: Take the location information for all data sets and puts it in GeoJson format

`dataTransform2.py`: Counts up the number of crimes and 311 requests according to zipcode

`dataTransform3.py`: Counts up the number of crimes and potholes according to zipcode
