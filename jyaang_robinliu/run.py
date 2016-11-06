#run.py
#get Data sets

#import data into MongoDB
import getHospitals
import getSchools
import getTestScores
import getCrimes
import getDayCamps

#Count Per Neighborhood
import hospitalsPerNeighborhood
import dayCampsPerNeighborhood
import schoolsPerNeighborhood

#Transformations
import mergeSchoolHospital
import mergeDayCampSchool
import mergeDayCampHospital
import cpiByDistrict
