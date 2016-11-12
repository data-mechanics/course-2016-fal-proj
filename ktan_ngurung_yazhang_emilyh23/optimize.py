from bs4 import BeautifulSoup
import requests
import re
import pandas as pd

# make a url request for boston population density retrieval 
url = 'http://www.city-data.com/zipmaps/Boston-Massachusetts.html'
user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.76 Safari/537.36"

headers = { 'User-Agent' : user_agent }
response = requests.get(url, headers=headers)
html = response.text
soup = BeautifulSoup(html)

# list of zipcodes
zc_list = ['02120', '02127', '02130', '02128', '02111', '02134', '02119', '02124', '02135', '02446', '02210', '02215', '02122', '02116', '02115', '02118', '02467', '02108', '02109', '02114', '02199', '02125']
zc_density_list = []

# parsing html
for zc in zc_list:
    zc_data_block = soup.find('div', {'id': zc})
    try:
        # getting the population density
        pop_str = zc_data_block.find('table').get_text()
        pop_numbers = re.findall('\d+', pop_str)
        pop_density = int(pop_numbers[0]+ pop_numbers[1] + '')
        
        # adding to list for dataframe transformation
        zc_density_list.append({'zc': zc, 'population_density': pop_density} ) 
    except AttributeError:
        
        # adding to list for dataframe transformation
        zc_density_list.append({'zc': zc, 'population_density': 22035})

zc_density_df = pd.DataFrame(zc_density_list)
zc_density_df