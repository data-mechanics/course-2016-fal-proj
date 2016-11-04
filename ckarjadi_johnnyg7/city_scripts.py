#same_street scripts
import math
def get_Col(db,repo):
    '''input: string representing database name db
       output: returns list with all key,values from database db
    '''
    col_list = []
    for elem in repo['ckarjadi_johnnyg7.' + db].find({}):
        col_list.append(elem)
    return col_list

def count_city(col,name):
    '''name = area; area is the city where the commGarden is in.'''
    fin =[]
    d={}
    check=[]
    #every x will have a dictionary {area: "city"}
    for x in col:
        city = x[name]
        if city not in check:
            d[city] = 1
            check+=[city]
        else:
            d[city]+=1
        
    #every x in d is a city name; every d[x] is the # of commGardens in that city
    for x in d:
        fin+=[{'city':x, 'num_comm_gardens':d[x]}]
    
    return [fin,check]
        
def count_food(col,name,compare,data, final):
    '''name = num_fast_food; compare=count_cities;  data = fast_food;
final = count_gardens.'''
    fin=[]
    d={}
    check=[]
    for x in col:
        
        try:
            business=x['businessname']
            city = x['city']
            if city in compare and business in data and city not in check:
                d[city]=1
                check+=[city]
            elif city in compare and business in data and city in check:
                d[city]+=1
        except KeyError:
            continue
    #every x in final is a dictionary, with city: "..."
    for x in final:
        city = x['city']
        if city in d:
            x[name]=d[city]
    return final
        
        









        
        
