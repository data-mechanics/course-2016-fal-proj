###############################
# HELPER FUNCTIONS

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

def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]


###############################
# MAIN

def run(M, P):
    OLD = []
    while OLD != M:
        OLD = M

        MPD = [(m, p, dist(m,p)) for (m, p) in product(M, P)]
        PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
        PD = aggregate(PDs, min)
        MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MT = aggregate(MP, plus)

        M1 = [(m, 1) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MC = aggregate(M1, sum)

        M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]
        print(sorted(M))

    return OLD
