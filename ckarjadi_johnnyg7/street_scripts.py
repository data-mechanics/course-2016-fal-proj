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

def get_waze(col,i_key,key2):
    
    d={}
    fin=[]
    check=[]
    for x in col:
        try:
            street = x[i_key].lower()
            calc = x['delay']
            # print(calc,street)
            if x[i_key] not in check:
                
                d[street]=[calc,1]
                
                
                check+=[x[i_key].lower()]
            else:
                d[street][0]+=calc

                d[street][1]+=1
        except KeyError:
            continue

    for x in d:
        #print(x,d[x][1])
        #print(d[x][0],d[x][1])
        #duration = int(d[x][0])//d[x][1]
        duration = d[x][1]
        fin+=[{i_key: x, 'num_jams': duration}]
    
    print([fin,check])
    return [fin,check]
def get_AWZ(col,i_key,compare,final):
    '''i_key = street'''
    
    d={}
    check=[]
    for x in col:
        try:
            street=x[i_key].lower()
            if street[-2:] == 'av':
                street+='e'
            
            if street not in check:
                d[street]=1
                check+=[street]
            else:
                d[street]+=1
            
##            if street in compare and street not in check:
##                d[i_key]= 1
##                check+=[i_key]
##            elif street in compare and street in check:
##                d[i_key]+=1
        except KeyError:
            continue
    
    for x in final:
        the_street = x['street']
        if the_street in d:
        
            x['num_active_zones']=d[x['street']]
    
            
##    for x in final:
##        if x['num_active_zones'] > 1 or x['num_jams'] > 1:
##            print(x)
##    for x in final:
##        the_street=x['street']
##        if the_street not in d:
##            print(the_street)
    return final





















    
