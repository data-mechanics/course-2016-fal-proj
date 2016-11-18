import urllib.request
import urllib.parse
import json
import dml
import prov.model
import datetime
import uuid

class HospitalLocationAnalysis(dml.Algorithm):
    contributor = 'ggelinas'
    reads = ['ggelinas.hospitals',
             'ggelinas.incidents']
    writes = []

    def dist(p, q):
        (x1, y1) = p
        (x2, y2) = q
        return (x1-x2)**2 + (y1-y2)**2

    def product(R, S):
        return [(t, u) for t in R for u in S]

    def plus(args):
        p = [0, 0]
        for (x,y) in args:
            p[0] += x
            p[1] += y
        return tuple(p)

    def scale(p, c):
        (x, y) = p
        return (x/c, y/c)

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ggelinas', 'ggelinas')

        Hospital = []
        Crime = []
        X = []
        Y = []

        for h in repo['ggelinas.hospitals'].find():
            try:
                if not (h['location']['coordinates'] == [0,0]):
                    Hospital.append((h['location']['coordinates'][1], h['location']['coordinates'][0]))
                    X.append(h['location']['coordinates'][1])
                    Y.append(h['location']['coordinates'][0])
            except KeyError:
                print('oh');

        count = 0
        for c in repo['ggelinas.incidents'].find():
            if trial:
                count += 1
                if count <= 200:
                    try:
                        if not (c['location']['coordinates'] == [0,0]):
                            Crime.append((c['location']['coordinates'][1], c['location']['coordinates'][0]))
                    except KeyError:
                        print('oops, something went wrong')
            else:
                try:
                    if not (c['location']['coordinates'] == [0,0]):
                        Crime.append((c['location']['coordinates'][1], c['location']['coordinates'][0]))
                except KeyError:
                    print('oops, something went wrong')

        old = []
        while old != Hospital:
            old = Hospital
            MPD = [(m, p, HospitalLocationAnalysis.dist(m,p)) for (m, p) in HospitalLocationAnalysis.product(Hospital, Crime)]
            PDs = [(p, HospitalLocationAnalysis.dist(m,p)) for (m,p,d) in MPD]
            PD = HospitalLocationAnalysis.aggregate(PDs, min)
            MP = [(m,p) for ((m,p,d), (p2,d2)) in HospitalLocationAnalysis.product(MPD, PD) if p==p2 and d==d2]
            MT = HospitalLocationAnalysis.aggregate(MP, HospitalLocationAnalysis.plus)
            M1 = [(m, 1) for ((m,p,d), (p2,d2)) in HospitalLocationAnalysis.product(MPD, PD) if p==p2 and d==d2]
            MC = HospitalLocationAnalysis.aggregate(M1, sum)
            M = [HospitalLocationAnalysis.scale(t,c) for ((m,t),(m2,c)) in HospitalLocationAnalysis.product(MT, MC) if m == m2]

        XM = [float(i[0]) for i in M]
        XY = [float(i[1]) for i in M]
        Xdiff = [abs(x-y) for x, y in zip(XM, X)]
        Ydiff = [abs(x-y) for x, y in zip(XY, Y)]
        XY = [(x,y) for x,y in zip(Xdiff, Ydiff)]

        print("this is the locations of current Hospital station: " + str(Hospital))
        print("K means Hospital locations: " + str(M))
        print("this is difference between Hospital and Clusters: " + str(XY))

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ggelinas', 'ggelinas')

        doc.add_namespace('alg',
                          'http://datamechanics.io/algorithm/ggelinas')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat',
                          'http://datamechanics.io/data/ggelinas')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ggelinas#numOfCrimeInDistricts',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        stations_resource = doc.entity('dat:ggelinas#stations', {'prov:label': 'Boston Police Stations District',
                                                                 prov.model.PROV_TYPE: 'ont:DataSet'})
        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime,
                                {'prov:label': 'Get Boston Police Stations District'})
        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(
            this_run,
            stations_resource,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Retrieval'}
        )

        incidents_resource = doc.entity('dat:ggelinas#incidents', {'prov:label': 'Crime Incidents Report',
                                                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                                                   'ont:Extension': 'json'})
        this_run2 = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                 {'prov:label': 'Get Crime Incidents District Report Data'})
        doc.wasAssociatedWith(this_run2, this_script)
        doc.usage(
            this_run2,
            incidents_resource,
            startTime,
            None,
            {prov.model.PROV_TYPE: 'ont:Computation'}
        )

        stations = doc.entity('dat:ggelinas#stations',
                              {prov.model.PROV_LABEL: 'Districts incident count', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, this_run, endTime)
        doc.wasDerivedFrom(stations, stations_resource, this_run, this_run, this_run)

        incidents = doc.entity('dat:ggelinas#incidents',
                               {prov.model.PROV_LABEL: 'Counted incidents', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(incidents, this_script)
        doc.wasGeneratedBy(incidents, this_run2, endTime)
        doc.wasDerivedFrom(incidents, incidents_resource, this_run2, this_run2, this_run2)

        repo.record(doc.serialize())
        repo.logout()

        return doc


HospitalLocationAnalysis.execute()
