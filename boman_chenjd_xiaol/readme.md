# course-2016-fal-proj
Author: Jiadong Chen
Project #1
I am going to investigate the relationship between a series of factor and crime incident locations. The first step is to union school garden and community garden, and I generate 10 clusters based on the geographical information via k-means. Then in the future I am able to fit regression model with crime location as y to see if there is any connection between them.(Relationship between garden locations and crime locations). Secondly, I repeat the steps as above to explore the connection between liquor locations and crime; property assessment and crime. The purpose of this project is to investigate the location with the highest crime rate, and so we are able to provide suggestions to police and prevent it. 

The algorithms and data manipulations are all stored in data_prep.py, which may seem messy right now. I going to seperate it into different files in next project so that it can be more clear which file does what job.

Running Instruction: python data_prep.py

Project #2
In this project, I uses the geojson datasets produced by k-means. (G:the garden locations, C:crime locations, L:liquor locations and P:property assessment locations, each represented as (latitude, longitude)), so I calculated the distance between each point in G to the closest point in C, and the reverse. If the average distance are all close and the standard deviations are both small, then I can determine that the garden locations are highly correlated with crime rate, and the same methodology can be applied to liquor store location and property assessment. The standard deviation of the shortest distance from gardens to crime is 295.5927, while the reverse is 0.2736. Since the STD is relatively large so far we cannot determine the correlation between them. Yet the STD of the shortest distance from crime locations to liquor store locations and the reverse are both very small, which are 0.3177 and 0.1385, so that we can say the liquor store locations are highly correlated to crime behaviors. 

As for property location, we firstly uses mornaltest from numpy to check the gross tax's distribution, in which the pvalue is 1.519e-310, so we can determine it is normally distributed, then we set up a filter that leaves the properties whose gross tax is gretaer than 1.28*zscore(top 10% of all), then we investigate the correlation between those and crime locations, find the STD 2.1829, 0.0742 reversely. So it is still a relatively small number, indicating the correlation between high-valued property and crime rate. 

After all, we are able to draw the conclusion that the liquor store location and high-valued property are correlated with crime rate. 

Overall, I run two statistics analysis(test correlation and generate top ten percent highest valued buildings via zscore) over the cluster obtained from proj 1.

In the future, we will apply more scientific algorithms including spatial test to compare the relationship between geographical data(two-dimensional dataset).
