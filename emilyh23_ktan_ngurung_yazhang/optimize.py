from bs4 import BeautifulSoup
import requests
import re
import datetime
import uuid
import dml
import numpy as np 
import warnings
import prov.model
import json

warnings.filterwarnings("ignore")

class optimize(dml.Algorithm):
    contributor = 'ktan_ngurung_yazhang_emilyh23'
    reads = ['ktan_ngurung_yazhang_emilyh23.zipcodeRatings']
    writes= []
    
    @staticmethod
    def execute():
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')
        
        global zc_ratings
        global zc_list 
        global zc_income_dict
        global zc_pop_dict
        
        # dictionary of zipcode ratings
        zc_ratings = repo.ktan_ngurung_yazhang_emilyh23.zipcodeRatings.find_one() 
        zc_ratings.pop('_id', None)
    
        zc_list = list(zc_ratings.keys()) # list of zipcodes
        # list of incomes for calculating low, med, and high
        income_list = []
        zc_income_dict = {}
        zc_pop_dict = {} # for sorting zipcodes by population
        
        # make url request for boston zipcode data retrieval 
        url = 'http://www.city-data.com/zipmaps/Boston-Massachusetts.html'    
        response = requests.get(url)
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
        
        # assigning income ranking to zip code 
        for zc in zc_list:
            zc_income = zc_income_dict[zc]['med_house_income']
        
            if zc_income < low:
                zc_income_dict[zc]['income_star'] = 'low'
            elif zc_income > high:
                zc_income_dict[zc]['income_star'] = 'high'
            else:
                zc_income_dict[zc]['income_star'] = 'med'
        optimize.user_input()
    
    @staticmethod     
    def find_sim_zipcode(u_bus, u_station, u_college, u_bigbelly, u_hubway, n):
        '''
        similarity threshold: 3 out of 5   
        sim_zc_pop contains all zipcodes that have met similiarity threshold    
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
            
            if (u_bus == zc_bus):
                sim_c+=1
            if (u_station == zc_station):
                sim_c+=1
            if (u_college == zc_college):
                sim_c+=1
            if (u_bigbelly == zc_bigBell):
                sim_c+=1
            if (u_hubway == zc_hubway):
                sim_c+=1
            
            # zipcodes being considered for optimization
            if sim_c >= 3:
                sim_zc_pop.append((zc, zc_pop_dict[zc]))
                sim_zc_data[zc] = {'overall_rating': ratings['overall_star'], 'income_star': zc_income_dict[zc]['income_star']}
        return optimize.zc_optimize(sim_zc_data, sim_zc_pop, n)
        
    @staticmethod
    def zc_optimize(sim_zc_data, sim_zc_pop, n):
        top_zc_data = {}
        # zipcodes sorted by population density in descending order
        sorted_sim_zc = sorted(sim_zc_pop, key=lambda x: x[1], reverse=True)

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
        
        print('Found {} zipcode(s) that most satisfy your search: '.format(zc_len))
        print(sorted_zc)
        print()
        
        return(top_zc_data, zc_len)
    
    @staticmethod
    # finds n zipcodes with similar cateogory ratings as user's specified ratings
    def user_query(bus_r, station_r, college_r, bigBelly_r, hubway_r, n):    
        results, zc_len = optimize.find_sim_zipcode(bus_r, station_r, college_r, bigBelly_r, hubway_r,n)
        print('Here are the top {}:'.format(n))
        
        for zc, data in results.items():
            print()
            print('zipcode: {}'.format(zc))
            print('{}\'s overall ranking: {}'.format(zc,data['overall_rating']))
            print('neighborhood income: {}'.format(data['income_star']))
     
    @staticmethod       
    # checks user input and raises errors if incorrect
    def check_rating(user_in):
        try:
            n = int(user_in)
        except ValueError:
            raise AssertionError('Not an integer, try again.')
        if (n != 1 and n != 2 and n != 3):
            raise AssertionError('Rating must be an integer: 1, 2, or 3.')
         
    @staticmethod
    def check_int(user_in):
        try:
            n = int(user_in)
        except ValueError:
            raise AssertionError('Not an integer, try again.')  
        if (n <= 0):
                raise AssertionError('Integer must be greater than 0.')
            
    @staticmethod
    def user_term(user_in):
        if user_in != 'y' and user_in != 'n':
            raise AssertionError('Enter y or n.') 
     
    @staticmethod
    def get_fields(cat_name):
        while True:
            try:    
                cat_rating = input('enter rating for {} (1,2,3): '.format(cat_name))
                optimize.check_rating(cat_rating)
                return(int(cat_rating))
            except AssertionError as e:
                print(str(e))
    
    @staticmethod
    def user_input():
        '''      
        ask for user input, check for formatting errors, user_query will return the zipcodes 
        that most satisfy the user's category rankings and maximizes population density.
        will keep asking input unless specified to quit.
        '''
        while True:
            another = ''
            try:
                n = input('enter number of areas you seek to advertise in: ')
                optimize.check_int(n)
                n = int(n)
                
                bus_r = optimize.get_fields('bus stops')
                station_r = optimize.get_fields('Tstops')
                college_r = optimize.get_fields('colleges')
                bigBelly_r = optimize.get_fields('Bigbelly')
                hubway_r = optimize.get_fields('Hubways')
                
                print()
                optimize.user_query(bus_r, station_r, college_r, bigBelly_r, hubway_r,n)
                print()
                
                while True:
                    try:
                        another = input('Find another? (y,n): ')
                        optimize.user_term(another)
                    except AssertionError as e:
                        print(str(e))
                    if (another == 'y' or another == 'n'):
                        break
                
            except AssertionError as e:
                print(str(e))
            if (another == 'n'):
                print('Goodbye.')
                return
    
    @staticmethod           
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:ktan_ngurung_yazhang_emilyh23#optimize', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        zipcodeRatings_resource = doc.entity('dat:ktan_ngurung_yazhang_emilyh23/zipcode-ratings', {'prov:label':'Critera Rating and Overall Rating for Zipcodes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Query'})

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, zipcodeRatings_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc
    
optimize.execute()
doc = optimize.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof