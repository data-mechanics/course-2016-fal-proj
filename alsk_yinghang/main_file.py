#CS591

#This portion of the code retrives 5 datasets from online sources. Takes 2-3 mins to run
# print("Running get_crime")
# import get_crime
# print("Running get_food_pantries")
# import get_food_pantries
# print("Running running get_lights")
# import get_lights
# print("Running get_police_stations")
# import get_police_stations
# print("Running get_properties")
# import get_properties
# print("Done wiht data retrieval!!!")

#This portion of the code manipulates the retrieved data and turn them into 3 informative datasets
# print("Data manipulation starts")
# print("Starting with crime_lights")
# import crime_lights
# print("Continuing with crime_police_station")
# import crime_police_station
#------------------------------------------------------------
#WARNING.......CRIME_PROPERTIES non-trial mode will take 10-15 mins to run
#------------------------------------------------------------
# print("Crime_properties")
# import crime_properties

print("Correlation & p-values for lights vs crimes")
import correlation_lights_crimes

print("Correlation & p-values for average property value vs crims")
import correlation_propval_crime


