import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code


class k_means(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = ['jzhou94_katerin.crime_incident']
    writes = ['jzhou94_katerin.k_means']
        
    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

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

        repo.dropPermanent('jzhou94_katerin.k_means')
        repo.createPermanent('jzhou94_katerin.k_means')

        M = [(-71.1097, 42.3736), (-71.05891, 42.3601)]
        P = [(doc['location']['coordinates'][0], doc['location']['coordinates'][1]) for doc in repo.jzhou94_katerin.crime_incident.find()]
        
        '''        
        OLD = []
        inRange = False;

        r = 2
        inRange = False

        while (inRange == False):
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
            inRange = True
            j = 0
            for i in M:
                if (M[j][0] <= (OLD[j][0] + r) and M[j][0] >= (OLD[j][0] - r) and M[j][1] <= (OLD[j][1] + r) and M[j][1] >= (OLD[j][1] - r)):
                    inRange = inRange and True
                else:
                    inRange = inRange and False
                j = j+1
        '''

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
            
        j = 0
        for i in M:
            repo['jzhou94_katerin.k_means'].insert({'Name': M[j]})
            j = j+1

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

         # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jzhou94_katerin/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/jzhou94_katerin/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:k_means', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        crime_incident = doc.entity('dat:crime_incident', {prov.model.PROV_LABEL:'Crime Incident', prov.model.PROV_TYPE:'ont:DataSet'})
        get_kMeans = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_kMeans, this_script)
        doc.usage(get_kMeans, crime_incident, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )

        kMeans = doc.entity('dat:k_means', {prov.model.PROV_LABEL:'K Means Location for Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(kMeans, this_script)
        doc.wasGeneratedBy(kMeans, get_kMeans, endTime)
        doc.wasDerivedFrom(kMeans, crime_incident, get_kMeans, get_kMeans, get_kMeans)


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

k_means.execute()
print("k_means Algorithm Done")
doc = k_means.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("k_means Provenance Done")

## eof
