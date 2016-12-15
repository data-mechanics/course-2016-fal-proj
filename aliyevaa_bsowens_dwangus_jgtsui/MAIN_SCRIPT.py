print("Importing file 1.")
import cleanup

print("Importing file 2.")
import combineRestaurantEnt as combine

print("Importing file 3.")
import correlation

#print("Importing file 4.") # unused by david
# import crimeRates

print("Importing file 5.")
import crimeRates_and_propertyVals_Faster_Aggregation as cRP

print("Importing file 6.")
import distancesCommunityScoreCalculations as communityScore

print("Importing file 7.")
import entertainment_full_retrieval as entertainments

print("Importing file 8.")
import food_full_retrieval as foods

print("Importing file 9.")
import gridCenters as centers

print("Importing file 10.")
import libraries

print("Importing file 11.")
import parking

print("Importing file 12.")
import prop_retr as properties

print("Importing file 13.")
import retrieveData

print("Importing file 14.")
import retrievingALLCrimesScript as crimes

print("Importing file 15.")
import scoreLocations as posnegScores


'''
Run scripts in this order:
	retrieveData.py -->
	entertainment_full_retrieval.py -->
	food_full_retrieval.py --> 
        libraries.py --> 
        parking.py --> 
        cleanup.py --> 
        combineRestaurantEnt.py --> 
        scoreLocations.py --> 
        gridCenters.py --> 
	retrievingALLCrimesScript.py --> 
	prop_retr.py --> 
        distancesCommunityScoreCalculations.py --> 
        crimeRates_and_propertyVals_Faster_Aggregation.py --> 
	correlation.py
'''
def nL():
    print("\n")

print("Now starting scripts.")

retrieveData.main()
nL()

entertainments.main()
nL()

cleanup.main()
nL()

foods.main()
nL()

combine.main()
nL()

libraries.main()   #getting throttled, lost that dataset
nL()

parking.main()     #getting throttled, but still have that dataset
nL()

posnegScores.main()
nL()

centers.main()
nL()

crimes.main()
nL()

properties.main()
nL()

communityScore.main()
nL()

cRP.main()
nL()

correlation.main()










