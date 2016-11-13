from bs4 import BeautifulSoup
import requests
import re
import datetime
import dml
import numpy as np 
import warnings
warnings.filterwarnings("ignore")

'''Retrieve some data sets (not using the API here for the sake of simplicity).'''
startTime = datetime.datetime.now()

# Set up the database connection.
client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')

# dictionary of zipcode ratings
zc_ratings = repo.ktan_ngurung_yazhang_emilyh23.zipcodeRatings.find_one() 
zc_ratings.pop('_id', None)

# list of zipcodes
zc_list = list(zc_ratings.keys())[1:]
# list of incomes for calculating low, med, and high
income_list = []
zc_income_dict = {}

# for sorting zipcodes by population
zc_pop_dict = {}

# make url request for boston zipcode data retrieval 
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
        # getting the median household income for zipcode
        b_text = zc_data_block.findAll(text=True)
        income_index = b_text.index('Estimated median household income in 2013:')
        med_house_income_str = b_text[income_index+1] 
        med_house_income = re.sub("[^0-9]", "", med_house_income_str)
      
        income_list.append(int(med_house_income))                
        
        # getting the population density for zipcode
        pop_str = zc_data_block.find('table').get_text()
        pop_numbers = re.findall('\d+', pop_str)
        pop_density = pop_numbers[0]+ pop_numbers[1]
        
        # storing scraped data
        zc_income_dict[zc] = {'med_house_income': int(med_house_income)}
        zc_pop_dict[zc] = int(pop_density)
        
    except AttributeError:
        if zc == '02446':
            income_list.append(79289)
            zc_income_dict[zc] = {'med_house_income': 79289}
            zc_pop_dict[zc] = 22035

# calculating high and low for income ranking
mean = np.mean(income_list, axis=None)
std = np.std(income_list, axis=0)
high = mean + std/1.5  
low = mean - std/1.5 

# assigning income ranking to zipcode 
for zc in zc_list:
    zc_income = zc_income_dict[zc]['med_house_income']

    if zc_income < low:
        zc_income_dict[zc]['income_star'] = 1
    elif zc_income > high:
        zc_income_dict[zc]['income_star'] = 3
    else:
        zc_income_dict[zc]['income_star'] = 2


def find_sim_zipcode(u_bus, u_station, u_college, u_bigbelly, u_hubway, n):
    '''
    threshold for similarity: 3 out of 5
    similarity score is penalized by -0.5 if a zipcode category rating = 1 
    but user imputs 2 or 3 (1 being the worst score possible)
    '''
    sim_zc_pop = []
    
    sim_zc_data = {}
    for zc in zc_list: 
        sim_c = 0 # keeps track of the number of times zipcode ranking for a category matches user input for that category
        
        # category ratings for zipcode
        ratings = zc_ratings[zc]
        zc_bus = ratings['bus_star']
        zc_station = ratings['station_star']
        zc_college = ratings['college_star']
        zc_bigBell = ratings['bigBelly_star']
        zc_hubway = ratings['hubway_star']
        
        # for testing
        print('zc   {}  {}  {}  {}  {}'.format(zc_bus,zc_station,zc_college,zc_bigBell,zc_hubway))
        print('user {}  {}  {}  {}  {}'.format(u_bus, u_station, u_college, u_bigbelly, u_hubway,))
        
        if (u_bus == zc_bus):
            sim_c+=1
        if (u_bus != 1 and zc_bus == 1):
            sim_c-=0.5
        if (u_station == zc_station):
            sim_c+=1
        if (u_bus != 1 and zc_station == 1):
            sim_c-=0.5
        if (u_college == zc_college):
            sim_c+=1
        if (u_college != 1 and zc_college == 1):
            sim_c-=0.5
        if (u_bigbelly == zc_bigBell):
            sim_c+=1
        if (u_college != 1 and zc_college == 1):
            sim_c-=0.5
        if (u_hubway == zc_hubway):
            sim_c+=1
        if (u_college != 1 and zc_college == 1):
            sim_c-=0.5

        # for testing        
        print('zc: {}, similarity rating: {}'.format(zc,sim_c))
        print()
        
        # zipcodes being considered for optimization
        if sim_c >= 3:
            sim_zc_pop.append((zc, zc_pop_dict[zc]))
            sim_zc_data[zc] = {'overall_rating': ratings['overall_star'], 'income_star': zc_income_dict[zc]['income_star']}
    return optimize(sim_zc_data, sim_zc_pop, n)
        
def optimize(sim_zc_data, sim_zc_pop, n):
    top_zc_data = {}
    # zipcodes sorted by population density in descending order
    sorted_sim_zc = sorted(sim_zc_pop, key=lambda x: x[1], reverse=True)

    # for testing
    print(sorted_sim_zc)
    
    # just zipcodes
    sorted_zc = [zc_pop[0] for zc_pop in sorted_sim_zc]
    zc_len = len(sorted_sim_zc)
    
    # top n zipcodes 
    if (zc_len < n):
        top_zc = sorted_zc        
    else:
        top_zc = sorted_zc[:n]
        
    for zc in top_zc:
        top_zc_data[zc] = {'overall_rating': sim_zc_data[zc]['overall_rating'], 'income_star': sim_zc_data[zc]['income_star']}
    
    return(top_zc_data, zc_len)

# finds n zipcodes with similar cateogory ratings as user's specified ratings
def user_query(bus_r, station_r, college_r, bigBelly_r, hubway_r, n):    
    results, zc_len = find_sim_zipcode(bus_r, station_r, college_r, bigBelly_r, hubway_r,n)
    print('Found {} zipcode(s) that most satisfy your search, here are the top {}:'.format(zc_len, n))
    
    for zc, data in results.items():
        print('zipcode: {}'.format(zc))
        print('{}\'s overall ranking: {}'.format(zc,data['overall_rating']))
        print('neighborhood income: {}'.format(data['income_star']))

# checks user input and raises errors if incorrect
def check_rating(user_in):
    try:
        n = int(user_in)
    except ValueError:
        raise AssertionError('Not an integer, try again.')
    if (n != 1 and n != 2 and n != 3):
        raise AssertionError('Rating must be an integer - 1, 2, or 3.')
       
def check_int(user_in):
    try:
        int(user_in)
    except ValueError:
        raise AssertionError('Not an integer, try again.')

def user_term(user_in):
    if user_in != 'y' and user_in != 'n':
        raise AssertionError('Enter y or n.') 
 
def get_fields(r_name):
    while True:
        try:    
            bus_r = input('enter rating for {} (1,2,3): '.format(r_name))
            check_rating(bus_r)
            return(int(bus_r))
        except AssertionError as e:
            print(str(e))

'''      
ask for user input, check for formatting errors, user_query will return the zipcodes 
that most satisfy the user's category rankings and maximizes population density.
will keep asking user for input unless specified to quit.
'''

while True:
    another = ''
    try:
        n = input('enter number of areas you seek to advertise in: ')
        check_int(n)
        n = int(n)
        
        bus_r = get_fields('bus stops')
        station_r = get_fields('Tstops')
        college_r = get_fields('colleges')
        bigBelly_r = get_fields('Bigbelly')
        hubway_r = get_fields('Hubways')
        
        print()
        user_query(bus_r, station_r, college_r, bigBelly_r, hubway_r,n)
        print()
        
        while True:
            try:
                another = input('Find another? (y,n) ')
                user_term(another)
            except AssertionError as e:
                print(str(e))
            if (another == 'y' or another == 'n'):
                break
        
    except AssertionError as e:
        print(str(e))
    if (another == 'n'):
        break