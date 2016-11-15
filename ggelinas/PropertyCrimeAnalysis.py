import urllib.request
import json
import dml
import prov.model
import uuid
from math import sqrt
from random import shuffle

class PropertyCrimeAnalysis(dml.Algorithm):
    contributor = 'ggelinas'
    reads = ['ggelinas.stations',
             'ggelinas.property']
    writes = []

    @staticmethod
    def permute(x):
        shuffled = [xi for xi in x]
        shuffle(shuffled)
        return shuffled

    @staticmethod
    def avg(x):
        return sum(x)/len(x)

    @staticmethod
    def stddev(x):
        m = avg(x)
        return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

    @staticmethod
    def cov(x, y):
        return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

    @staticmethod
    def corr(x, y):
        if stddev(x)*stddev(y) != 0:
            return cov(x, y)/(stddev(x)*stddev(y))

    @staticmethod
    def p(x, y):
        c0 = corr(x, y)
        corrs = []
        for k in range(0,2000):
            y_permuted = permute(y)
            corrs.append(corr(x, y_permuted))
        return len([c for c in corrs if abs(c) > c0])/len(corrs)


