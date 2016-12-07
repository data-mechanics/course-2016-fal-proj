import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class police_location_analysis(dml.Algorithm):
    contributor='manda094_nwg_patels95'
    reads = ['manda094_nwg_patels95.district_crimes', 'manda094_nwg_patels95.crimes']
    writes =[]

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


        Means = []
        Points = []
        X = []
        Y = []

        for d in repo['manda094_nwg_patels95.district_crimes'].find():
            try:
                if not (d["geometry"]["coordinates"] == [0,0]):
                    Means.append((d["geometry"]["coordinates"][1], d["geometry"]["coordinates"][0]))
                    X.append(d["geometry"]["coordinates"][1])
                    Y.append(d["geometry"]["coordinates"][0])
            except KeyError:
                print()

        count = 0
        for p in repo['manda094_nwg_patels95.crimes'].find():
            if trial:
                count += 1
                if count <= 200:
                    try:
                        if not (p["location"]["coordinates"] == [0,0]):
                            Points.append((p["location"]["coordinates"][1], p["location"]["coordinates"][0]))

                    except KeyError:
                        print()
            else:
                try:
                    if not (p['location']['coordinates']==[0,0]):
                        Points.append((p["location"]["coordinates"][1], p["location"]["coordinates"][0]))
                except KeyError:
                    print()



        OLD = []
        while OLD != Means:
            OLD = Means
            MPD = [(m, p, police_location_analysis.dist(m,p)) for (m, p) in police_location_analysis.product(Means, Points)] #returns mean and point and distance between each
            PDs = [(p, police_location_analysis.dist(m,p)) for (m, p, d) in MPD] #returns point and distance between mean and points
            PD =  police_location_analysis.aggregate(PDs, min)
            MP = [(m, p) for ((m,p,d), (p2,d2)) in police_location_analysis.product(MPD, PD) if p==p2 and d==d2]
            MT = police_location_analysis.aggregate(MP, police_location_analysis.plus)
            M1 = [(m, 1) for ((m,p,d), (p2,d2)) in police_location_analysis.product(MPD, PD) if p==p2 and d==d2]
            MC = police_location_analysis.aggregate(M1, sum)
            M = [police_location_analysis.scale(t,c) for ((m,t),(m2,c)) in police_location_analysis.product(MT, MC) if m == m2]



        XM = [float(i[0]) for i in M] #pulling out all the X values from M
        YM = [float(i[1]) for i in M] #pulling out all Y values from M
        XDiff= [abs(x - y) for x, y in zip(XM, X)] #Finding the abs difference between clustered means and data district
        YDiff= [abs(x - y) for x,y in zip (YM, Y)]
        XY = [(x,y) for x,y in zip(XDiff, YDiff)] #combining calculated differences into one list of tuples (x,y)


    #    print("This is the locations of the current Police stations:  " + str(Means) + '\n')
#        print("K Means Police station locations:" + str(M) + '\n')
#        print("This is the absolute difference between Means and Clustered Means:" + str(XY) + '\n')


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}, Means, X,Y, XM, YM, M


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

        this_script = doc.agent('alg:manda094_nwg_patels95#police_location_analysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Police Location Analysis Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        doc.usage(this_run, resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        k_clusters = doc.entity('dat:manda094_nwg_patels95#police_location_analysis', {prov.model.PROV_LABEL:'Police Analysis Report', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_clusters, this_script)
        doc.wasGeneratedBy(k_clusters, this_run, endTime)
        doc.wasDerivedFrom(k_clusters, resource, this_run, this_run, this_run)

        repo.record(doc.serialize())
        repo.logout()

        return doc

police_location_analysis.execute()
doc = police_location_analysis.provenance()
analysis_data = police_location_analysis.execute()



#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
