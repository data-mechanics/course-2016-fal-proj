from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import datetime
import json
import dml
import numpy as np 

'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
startTime = datetime.datetime.now()

# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')

zc_ratings = repo.ktan_ngurung_yazhang_emilyh23.zipcodeRatings.find_one() 

# list of zipcodes
zc_list = list(zc_ratings.keys())[1:]
zc_density_list = []
# list of incomes for calculating low, med, and, high
zc_income_list = []

# make a url request for boston population density retrieval 
url = 'http://www.city-data.com/zipmaps/Boston-Massachusetts.html'
user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.76 Safari/537.36"

headers = { 'User-Agent' : user_agent }
response = requests.get(url, headers=headers)
html = response.text
soup = BeautifulSoup(html)

# parsing html
for zc in zc_list:
    zc_data_block = soup.find('div', {'id': zc})
    
    try:
        # getting the medium household income
        b_text = zc_data_block.findAll(text=True)
        income_index = b_text.index('Estimated median household income in 2013:')
        med_house_income_str = b_text[income_index+1] 
        med_house_income = re.sub("[^0-9]", "", med_house_income_str)

        zc_income_list.append(int(med_house_income))        
        
        # getting the population density
        pop_str = zc_data_block.find('table').get_text()
        pop_numbers = re.findall('\d+', pop_str)
        pop_density = pop_numbers[0]+ pop_numbers[1] + ''
        
        # adding to list for dataframe transformation
        zc_density_list.append({'zc': zc, 'population_density': pop_density, 'median_household_income': med_house_income}) 
        
    except AttributeError:
        # adding to list for dataframe transformation
        zc_density_list.append({'zc': zc, 'population_density': '22035', 'median_household_income': '79289'})

print(zc_income_list)
mean = 0
zc_density_df = pd.DataFrame(zc_density_list)
print(zc_density_df)
print(zc_ratings)

def find_sim_zipcode():
    sim_c = 0
    pass

def optimize():
    print('working')

def user_query(bus_r, station_r, college_r, bigBelly_r, hubway_r):
    find_sim_zipcode()
    return optimize()
    
user_query(1,1,2,2,3)
