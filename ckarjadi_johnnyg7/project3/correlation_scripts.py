from random import shuffle
from math import sqrt
from convert import *
##data = [(18, 28), (24, 18), (27, 31), (14, 15), (46, 23),
##        (36, 19), (27, 10), (34, 25), (19, 15), (13, 13),
##        (4, 2), (17, 20), (28, 12), (36, 11), (26, 14),
##        (19, 19), (24, 13), (25, 6), (20, 8), (17, 22),
##        (18, 8), (25, 12), (28, 27), (31, 28), (35, 22),
##        (17, 8), (19, 19), (23, 23), (22, 11)]
##x = [xi for (xi, yi) in data]
##y = [yi for (xi, yi) in data]
def get_Col(db,repo):
    '''input: string representing database name db
       output: returns list with all key,values from database db
    '''
    col_list = []
    for elem in repo['ckarjadi_johnnyg7.' + db].find({}):
        col_list.append(elem)
    return col_list
def get_Short(db, repo,limit):
        '''input: string representing database name db
        
       output: returns list with all key,values from database db
       '''
        assert(limit >0)
        col_list = []
        count = 0
        for elem in repo['ckarjadi_johnnyg7.' + db].find({}):
                count+=1
                col_list.append(elem)
                if count == limit:
                        break
                        
        return col_list


def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

def p(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 3000):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return [len([c for c in corrs if (c) > (c0)])/len(corrs),c0]
def count(collection,zip_string):
    '''inputs:
collection = given data set collection
collection = [{zip_code: ##}, {zip_code:##}]

output:
unique = {zipcode: # of occurrences of collection incidents}
e.g. {01701: 10} for food pantries = 10 food pantries in zip
code 01701.

'''
  
    unique = {}
    for area in collection:
        
        zip_code = area[zip_string]
        if zip_code not in unique:
            unique[zip_code] = 1
        elif zip_code in unique:
            unique[zip_code]+=1
    return unique

            
            
        
def grab(data):
    x = [xi for (xi, yi) in data]
    y = [yi for (xi, yi) in data]
    return [x,y]
def create_intersect(col1, col2, key1):
    '''key1: zip_code
       key2: '0' + zipcode
       '''
    final = []
    for x in col1:
        d={}
        zip_code = x[key1]
        value = x['avg']
        if zip_code in col2:
            num_hospitals = col2[zip_code]
            d['num_hospitals'] = num_hospitals
            d['zip_code'] = zip_code
            d['avg'] = value
            final+=[d]
    return final
            
def match_values(average,zip_string,number):
    '''input:
average = [{zipcode, average property value}];
zip_string = given data set's entry for zip_code
number = {zipcode: # of occurrences of collection incidents}
e.g. {01701: 10} for food pantries = 10 food pantries in zip
code 01701.
'''
    #print(average)
    #print(number)
    final = []
    for avg in average:
        zip_code = avg['zip_code']
        value = avg['avg']
        if zip_code in number:
            num = number[zip_code]
            #num = number of occurrences in that zip_code
            pair = (value,num)
            final+=[pair]
    return final
def change_hospitals(collection):
    
    final = []
    for area in collection:
        if 'zipcode' in area:
            z_c = area['zipcode']
            
            area['zipcode'] = '0' + z_c
            final+=[area]
    return final

def change_gardens(collection):
    '''have to pad change_garden zip codes with a '0'
'''
    final = []
    for area in collection:
        z_c = area['zip_code']
        if len(z_c) == 4:
            area['zip_code'] = '0' + z_c
            final+=[area]
    return final

def change_food(collection):
    '''selecting just fast_food'''
    fast_food= ['Burger King', "McDonald's","Dunkin' Donuts","Starbucks Coffee"\
                ,"Chipotle Mexican Grill", "JIMMY JOHN'S", "Subway", "Domino's Pizza"\
                ,"Popeyes"]
    final = []
    for area in collection:
        d={}
        if len(area) == 3:
            z_c = area['zip']
            food = area['businessname']
         
           
                       
            if food in fast_food:
                
                d['zip_code'] = z_c
                final+=[d]
    return final
    
        
    

def change_crime(collection):
    '''converting districts to zipcodes'''
    final = []
    count = 0
    count2 = 0
    for area in collection:
        d= {}
        if 'district' in area:
            district = area['district']
            z_c = convert(district)
            d['zip_code'] = z_c
            final+=[d]
            if district == 'A1':
                count+=1
            else:
                count2+=1

    return final









    



