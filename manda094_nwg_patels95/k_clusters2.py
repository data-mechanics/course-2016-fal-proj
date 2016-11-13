import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class k_clusters2(dml.Algorithm):
    contributor='manda094_nwg_patels95'
    reads = ['manda094_nwg_patels95.district_crimes', 'manda094_nwg_patels95.crimes']
    writes =['manda094_nwg_patels95.k_clusters2']
    
    def dist(p, q):
        (x1,y1) = p
        (x2,y2) = q
        return (x1-x2)**2 + (y1-y2)**2

    def product(R,S):
        return [(t,u) for t in R for u in S]

    def plus(args):
        p = [0,0]
        for (x,y) in args:
            p[0] += x
            p[1] += y
        return tuple(p)

    def scale(p, c):
        (x,y) = p
        return (x/c, y/c)

    def aggregate(R, f):
       keys = {r[0] for r in R}
       return [(key, f([v for (k,v) in R if k == key])) for key in keys]


  
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')


        M = [(13,1), (2,12)]
        P = [(1,2),(4,5),(1,3),(10,12),(13,14),(13,9),(11,11)]

#        data = []

        #for d in repo['manda094_nwg_patels95.district_crimes'].find():
 #           for c in d['geometry']:
 #               for (y,x) in d['coordinates']:
 #                   data.append({'x_coordinate':x, 'y_coordinate':y})
 #                  print(data)

        data2 = []
        d = repo['manda094_nwg_patels95.district_crimes'].find_one()
        e = d['location'].find_one()
        for (x,y) in e['coordinates']:
            data2.append({'x':x, 'y':y})
            print(data2)

        data = []
        d= repo['manda094_nwg_patels95.crimes'].find_one()
        for 
        


        OLD = []
        while OLD != M:
            OLD = M
            MPD = [(m, p, k_clusters2.dist(m,p)) for (m, p) in k_clusters2.product(M, P)]
            PDs = [(p, k_clusters2.dist(m,p)) for (m, p, d) in MPD]
            PD =  k_clusters2.aggregate(PDs, min)
            MP = [(m, p) for ((m,p,d), (p2,d2)) in k_clusters2.product(MPD, PD) if p==p2 and d==d2]
            MT = k_clusters2.aggregate(MP, k_clusters2.plus)
            M1 = [(m, 1) for ((m,p,d), (p2,d2)) in k_clusters2.product(MPD, PD) if p==p2 and d==d2]
            MC = k_clusters2.aggregate(M1, sum)
            M = [k_clusters2.scale(t,c) for ((m,t),(m2,c)) in k_clusters2.product(MT, MC) if m == m2]
            print(sorted(M))


            
        repo.dropPermanent("k_clusters2")
        repo.createPermanent("k_clusters2")
 #       repo['manda094_nwg_patels95.k_clusters2'].insert_many(M)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}


    

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('manda094_nwg_patels95', 'manda094_nwg_patels95')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:manda094_nwg_patels95#k_clusters2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Cluster Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        k_clusters = doc.entity('dat:manda094_nwg_patels95#k_clusters2', {prov.model.PROV_LABEL:'Crimes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_clusters, this_script)
        doc.wasGeneratedBy(k_clusters, this_run, endTime)
        doc.wasDerivedFrom(k_clusters, resource, this_run, this_run, this_run)

        repo.record(doc.serialize())
        repo.logout()

        return doc

k_clusters2.execute()
doc = k_clusters2.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
    
