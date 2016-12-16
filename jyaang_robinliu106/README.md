Project #2 By Ji Eun Yang and Hin Lok Liu (jyaang_robinliu106)

For our project, we wanted to be able to measure child-friendliness per Boston neighborhood. We created a score for the following Boston neighborhoods: Allston, Back Bay, Bay Village, Beacon Hill, Brighton, Charlestown, Chinatown, Dorchester, Downtown Crossing, East Boston, Fenway, Hyde park, Jamaica Plain, Mattapan, Mission Hill, North End, Roslindale, Roxbury, South Boston, South End, West End, and Roxbury.

For each of these neighborhoods, we created a constraint satisfaction problem where we counted the number of schools and crimes within an approximate radius within the center of each neighborhood. We also created an optimization problem to find the hospital with the shortest distance from the center of the neighborhood.

We used datasets from the City of Boston to retrieve coordinates of all the hospitals, schools, crimes, and property values in Boston.

After calculating the number of schools and crimes, the distance to the closest hospital, and average property values we used these constants to developed a scoring algorithm based on our working weights: 25% hospitals, 25% education, 25% safety, and 25% housing costs. Based on these numbers, we generate a weighted score where a high number of schools, low number of crimes, short distance to a hospital, and low housing costs is desirable. A higher rating for a neighborhood indicates it is more child-friendly. Using this algorithm, we calculated scores for each neighborhood and stored it in a new collection “neighborhood_scores” in repo.

To set up our database:

in jyaang_robin_liu106/python/
$ python run.py

To view our page:

$ npm install
$ node app.js

In your Browser, go to "localhost:8000" to view page.

