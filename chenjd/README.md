# course-2016-fal-proj
Author: Jiadong Chen

I am going to investigate the relationship between a series of factor and crime incident locations. The first step is to union school garden and community garden, and I generate 10 clusters based on the geographical information via k-means. Then in the future I am able to fit regression model with crime location as y to see if there is any connection between them.(Relationship between garden locations and crime locations). Secondly, I repeat the steps as above to explore the connection between liquor locations and crime; property assessment and crime. The purpose of this project is to investigate the location with the highest crime rate, and so we are able to provide suggestions to police and prevent it. 

The algorithms and data manipulations are all stored in data_prep.py, which may seem messy right now. I going to seperate it into different files in next project so that it can be more clear which file does what job.

Running Instruction: python data_prep.py