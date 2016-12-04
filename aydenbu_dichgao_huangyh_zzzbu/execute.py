import os
from helpers import *


# To get all data sets we need
os.system('python3.5 ./get_data.py')

# Pre process each data set
os.system('python3.5 ./countSchool.py')
os.system('python3.5 ./countHospitals.py')
os.system('python3.5 ./countCommunityGardens.py')
os.system('python3.5 ./countHealthyCornerStores.py')
os.system('python3.5 ./zipAverageEarning.py')

# Merge the datas
os.system('python3.5 ./merge_school_garden.py')
os.system('python3.5 ./merge_store_hospital.py')
os.system('python3.5 ./merge_all_public_buildings.py')
os.system('python3.5 ./public_earning.py')

# Project 2
os.system('python3.5 ./zipXY.py')
os.system('python3.5 ./merge_crime_zip.py')
os.system('python3.5 ./countCrime.py')
os.system('python3.5 ./mergeAll.py')
os.system('python3.5 ./statisticOperations.py')
# os.system('python3.5 ./public_earning.py')