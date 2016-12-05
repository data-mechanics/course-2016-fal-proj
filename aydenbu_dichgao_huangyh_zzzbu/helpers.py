import json
import dml

def getAuth(key):
    with open('../auth.json') as data_file:
        data = json.load(data_file)
        return data[key]

def openDb(u, p):
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate(u, p)
    return repo



# Helper Method
def product(R, S):
    return [(t, u) for t in R for u in S]


def aggregate(R, f):
    keys = {r['zip'] for r in R}
    return [(key, f([r for r in R if r['zip'] == key])) for key in keys]


def dist(p, q):
    (x1, y1) = p
    (x2, y2) = q
    return (x1 - x2) ** 2 + (y1 - y2) ** 2


def plus(args):
    p = [0, 0]
    c = len(args)
    for i in args:
        p[0] += i['latitude']
        p[1] += i['longitude']
    return tuple(p)


def scale(p, c):
    (x, y) = p
    return (x / c, y / c)
