import numpy
import math
import json

def coordKMeans (initial, points):
    M = initial
    P = points
    OLD = []
    #DIFF = []
    notEqual = True
    while notEqual:
        ##print(OLD == M)
        ct = 0
        breakFlag = 0
        if OLD != []:
            for i in range(0, len(M)):
                print(i)
                if(math.isclose(OLD[i][0], M[i][0], rel_tol=1e-9) and math.isclose(OLD[i][1], M[i][1], rel_tol=1e-9)):
                    ct = ct + 1
                    if(i == len(M) - 1):
                        notEqual = False
                    continue
                else:
                    print(str(OLD[i][0]) + ',' + str(OLD[i][1]) + " not equal to " + str(M[i][0]) + ',' + str(M[i][1]))
                    breakFlag = breakFlag + 1
                    break
        print(ct)
        print("breakFlag: " + str(breakFlag))
        OLD = M
        #print("Start")
        MPD = [(m, p, dist(m,p)) for (m, p) in product(M, P)]
        PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
        PD = aggregate(PDs, min)
        MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MT = aggregate(MP, plus)
        M1 = [(m, 1) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MC = aggregate(M1, sum)
        M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]
        print(sorted(M))

    return M

##Project vector u onto vector v
def vectorProject(u, v):
    uv_Dot = (u[0]*v[0]) + (u[1] *v[1])
    vv_Dot = (v[0] * v[0]) + (v[1] * v[1])

    scale = float(uv_Dot) / float(vv_Dot)
    return (scale * v[0], scale * v[1]) 

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

def printjson(a):
    print(json.dumps(a, sort_keys=True, indent=4, separators=(',',': ')))

def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

#Credit to https://gist.github.com/rochacbruno/2883505#file-haversine-py-L6
def geoDist(p1,p2):
    long1, lat1 = p1
    long2, lat2 = p2
    radius = 6371 #in kilometer
    dlon = math.radians(long1-long2)
    dlat = math.radians(lat1-lat2)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c
    return d


#This function helps to transform the string coordinate to a tuple of (longitude, latitude)
def stringLocToNumLoc(s):
    tempS = s[1:-1]
    sTuple = tempS.split(',')
    return (float(sTuple[0]), float(sTuple[1]))

def houseEval(price, distToCrime, distToEntertain):
    Z = (25 - distToEntertain) * 10 - (3 + distToCrime) * 10 - (price) * 0.5
    return Z