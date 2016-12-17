README

First start the mongoDB with command mongod -- auth --dbpath “YOUR DB PATH” first navigate to the directory ll0406_siboz on terminal or command line.
The algorithm of this Project is divided to four parts:

1st part: proj1.py

2nd part: proj2.py

3rd part: part3.py, part3_2.py

4th part: part4.py

Data Retrieval and General Helper: dataRetrieval.py, generalHelperTemplate.py

There are separate folder of local data. There are several random sampled we generated before and stored them as json files in it. The purpose of having the local data for now is to produce a consistent K-means algorithm output, and later vector projections.

All the provenance data is included in the plan.json file 
Detail descriptions are commented out in each of the file

To run each algorithm, simply type code
python xxx.py
in the terminal or command line. However, be aware some of the algorithm may take up to 20 mins to run.


Description on Project 1:
The Project 1 serves as a introduction of this project. In proj1.py, the algorithm used data of Boston Crime Incidents from 2012-2016 and calculated the cluster points with K-means (k = 9) and examined how close police stations are to those crime clusters. It was quiet interestin too see that nearly all crime clusters have one police station nearby them within 2 kilometers.

Description on Project 2:
Project we focused on two problems:
1.	Examine the possible relationships between the shifting of the cluster points of Liquor Stores and the shifting of the cluster points of Shooting Crimes.
2.	How effective the Police Stations are from a Constraint Satisfaction view.
To solve the first one, we calculated the cluster points, using K-Means, of liquor stores and shooting incidents for 2013 and 2013-2016 respectively. By adding new data to the 2013 datasets, the cluster points of both liquor stores and shooting incidents will shift to different directions with different magnitude. What we are trying to do here is to find the changing of vector magnitude of Shooting Crime in the direction of changing of Liquor Store, and use correlation coefficients to determine if such changing of cluster points are related or not. As the result, the correlation coefficients we obtained is around -0.006 and we concluded that the moving liquor stores cluster points has little explanation to do with the moving of the shooting crimes.
Then we used constraint satisfaction technique. From the results we get in part 1, we know that there's somehow a trend that the location of shooting moves with the moving of liquor stores. Thanks to the project 1, we already have all the police station locations in Boston and we could use them to determine how efficient the police station was by comparing the trend of criminal location moving with the location of police station. We set an objective function Z = (10 * distance changed) km, and Z>= 5. That is to say, shooting report moved at least 500 meters then the nearby police station is named efficient. However, our result shows that none of those police station satisfied the constraint thus our conclusion is that there was no police station stayed efficient during 2013-2016.

Description on Part 3:
Due to the limit success of Project 2, we decided to experiment more on the algorithm we developed with Project 2 with other factors. And in the Code of part3.py and part3_2.py, we retreieved data from Property Assessment 2014, 2015 and 2016, and selected out a list of "low-value properties" and we calculated the cluster points for those low-value properties. Then the algorithm proceeded to calculate the crime clusters for each year from 2014 to 2016. All the k-means algorithms are in part3.py, and it takes quiet large amount of time to run it, and all the vector calculations are in part3_2.py.

Description on Part 4:
In part4.py, we implmented one rather simple residential buildings scoring system based on their price, distance to entertainment clusters and distance to crime clusters. Although the scoring system itself may not be very sophisticated, we can observe some general pattern to the map on the map.

And at last in Visualization folder, we implmented, using d3 and html,  two visualizations of part3 and part4 of this project. You can just load them directly to your browser to see the code in action, just be sure to download the entire folder of visualization together since d3 code are included inside.

Details of this project can be found in the final report .
