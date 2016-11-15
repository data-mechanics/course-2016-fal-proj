import random

def change_hospitals(col):
    '''
col = hospitals

preparing for future calculations

output:
final = {name: [point,zip_code]}

'''
    final = {}
    for hosp in col:
        lat = hosp['location']['coordinates'][1]
        long = hosp['location']['coordinates'][0]
        point = (lat,long)
        
        zip_code = hosp['zipcode']
        if len(zip_code) == 4:
            zip_code = '0' + zip_code
        name = hosp['name']
        if name not in final:
            final[name]=[point,zip_code,0,0]
    return final
    

def get_Col(db,repo):
    '''input: string representing database name db
       output: returns list with all key,values from database db
    '''
    col_list = []
    for elem in repo['ckarjadi_johnnyg7.' + db].find({}):
        col_list.append(elem)
    return col_list
def closest_crimes(col1,col2):
    '''input col1 = change_hospitals
col2 = crime collection

output:
{ name: [(lat,long), zipcode, num_crimes], name2: ... }

'''

    for crime in col2:
        if len(crime) != 4:
            continue
        
        crime_lat = float(crime['lat'])
        crime_long = float(crime['long'])
        crime_point = (crime_lat,crime_long)
        crime_dists = []
        for hosp in col1:
            distance = dist(crime_point,col1[hosp][0])
            crime_dists+=[[distance,hosp]]
        crime_closest = min(crime_dists)
        close_hosp = crime_closest[1]
##       
##        if len(col1[close_hosp]) == 2:
##            col1[close_hosp]+=[1]
##  
##        else:
        col1[close_hosp][2] +=1
        #add onto the total crimes for that hospital location

    return col1
def closest_jams(col1,col2):
    '''input col1 = closest_crimes result for hospitals
col2 = jams collection '''
    for jams in col2:
        if len(jams) !=3:
            continue
        jam_lat = float(jams['locy'])
        jam_long = float(jams['locx'])
        jam_point = (jam_lat,jam_long)
        jam_dists=[]
        for hosp in col1:
            distance = dist(jam_point,col1[hosp][0])
            jam_dists += [[distance,hosp]]
        jam_closest = min(jam_dists)
        close_hosp = jam_closest[1]
        col1[close_hosp][3]+=1
    return col1
        
 

def change_cluster(col1):
    '''col1 = {name: [(lat,long), zipcode, num crimes, num jams]}'''
    final = []
    for x in col1:
        add = col1[x][2]+col1[x][3]
        lat = col1[x][0][0]
        long = col1[x][0][1]
        point = (lat,long,add)
        final+=[point]
    return final
def create_means(points,n):
    ''' points = (lat,long,numcrimes+numjams)
n = number o hospitals to add
'''
    M = []
    lats = [ x for (x,y,z) in points]
    longs = [y for (x,y,z) in points]
    nums = [z for (x,y,z) in points]
    
    for x in range(n):
        lat = random.choice(lats)
        long = random.choice(longs)
        num = random.choice(nums)
        M +=[(lat,long,num)]
        
    return M
    
def k_means(M,P):
    ''' points = (lat,long,numcrimes+numjams)
n = number of hospitals to add

'''
    OLD = []
    while OLD != M:
        OLD = M

        MPD = [(m, p, dist_3d(m,p)) for (m, p) in product(M, P)]
        PDs = [(p, dist_3d(m,p)) for (m, p, d) in MPD]
        PD = aggregate(PDs, min)
        MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MT = aggregate(MP, plus)

        M1 = [(m, 1) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MC = aggregate(M1, sum)

        M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]
    return sorted(M)

def change_k_means(means):
    '''converts means into a dictionary that can be put into mongodb; has the
lat / long coordinates for the N hospitals to be built'''
    final = {}
    count = 0
    for x in means:
        final['hospital '+str(count)] = (x[0],x[1])
        count+=1
    return [final]
def plus(args):
    p = [0,0,0]
    for (x,y,z) in args:
        p[0] += x
        p[1] += y
        p[2]+=z
    return tuple(p)

def scale(p, c):
    (x,y,z) = p
    return (x/c, y/c,z/c)


def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]




def dist_3d(p,q):
    (x1,y1,z1) = p
    (x2,y2,z2) = q
    return ((x1-x2)**2 + (y1-y2)**2 + (z1-z2)**2)**(1/2)
    

    
        
def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return ((x1-x2)**2 + (y1-y2)**2)**(1/2)
