import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code


print("Getting Data")
import getData
getData
    
print("Merging all Crimes")
import mergeAllCrimes
mergeAllCrimes

print("Merging all Crimes that are not Drug Crimes")
import mergeAllWithoutDrugCrimes
mergeAllWithoutDrugCrimes

print("Merging all Drug Crimes")
import mergeDrugCrimes
mergeDrugCrimes

print("Merging all Children Establishments")
import mergeChildren
mergeChildren

print("Merging all Schools")
import mergeSchools
mergeSchools

