from random import shuffle
from math import sqrt
import trends

def get_pscores(trends):
    pscores = []
    for trend in trends:
        data = trend
        x = [xi for (xi, yi) in data]
        y = [yi for (xi, yi) in data]
        pscores += [pscore(x,y)]
    return list(zip(trends,pscores))

def dist(p,q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

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
    val =  sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)
    #print(val)
    return val

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        val = cov(x, y)/(stddev(x)*stddev(y))
        #print(val)
        return(val)
    else:
        print('Standard Deviation = 0')

def pscore(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 2000):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return len([c for c in corrs if abs(c) > c0])/len(corrs)