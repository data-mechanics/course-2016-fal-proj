import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *


class k_means(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.zip_Healthycornerstores_XY',
             'aydenbu_huangyh.zip_communityGardens_XY',
             'aydenbu_huangyh.zip_hospitals_XY',
             'aydenbu_huangyh.zip_PublicSchool_XY']
    writes = ['aydenbu_huangyh.k_means']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        # Get the collections
        stores = repo['aydenbu_huangyh.zip_Healthycornerstores_XY']
        gardens = repo['aydenbu_huangyh.zip_communityGardens_XY']
        hospitals = repo['aydenbu_huangyh.zip_hospitals_XY']
        schools = repo['aydenbu_huangyh.zip_PublicSchool_XY']

        stores_xy = []
        for document in stores.find():
            if document is not None:
                record = (float(document['location'][0]), float(document['location'][1]))
                stores_xy.append(record)

        gardens_xy = []
        for document in gardens.find():
            if document is not None:
                record = (float(document['location'][0]), float(document['location'][1]))
                if float(document['location'][0]) <0:
                    print ("gardens")
                gardens_xy.append(record)

        hospitals_xy = []
        for document in hospitals.find():
            if document is not None:
                record = (float(document['location'][0]), float(document['location'][1]))
                if float(document['location'][0]) < 0:
                    print ("hospitals")
                hospitals_xy.append(record)

        schools_xy = []
        for document in schools.find():
            if document is not None:
                record = (float(document['location'][0]), float(document['location'][1]))
                if float(document['location'][0]) < 0:
                    print ("schools")
                schools_xy.append(record)






        # k means algorithm code

        def product(R, S):
            return [(t, u) for t in R for u in S]

        def aggregate(R, f):
            keys = {r[0] for r in R}
            return [(key, f([v for (k, v) in R if k == key])) for key in keys]


        def dist(p, q):
            (x1, y1) = p
            (x2, y2) = q
            return (x1 - x2) ** 2 + (y1 - y2) ** 2

        def plus(args):
            p = [0, 0]
            for (x, y) in args:
                p[0] += x
                p[1] += y
            return tuple(p)

        def scale(p, c):
            (x, y) = p
            return (x / c, y / c)

        M = [(41, -70), (40, -70)]
        P = stores_xy + gardens_xy + hospitals_xy + schools_xy
        print(P)

        OLD = []
        while OLD != M:
            OLD = M

            MPD = [(m, p, dist(m, p)) for (m, p) in product(M, P)]
            PDs = [(p, dist(m, p)) for (m, p, d) in MPD]
            PD = aggregate(PDs, min)
            MP = [(m, p) for ((m, p, d), (p2, d2)) in product(MPD, PD) if p == p2 and d == d2]
            MT = aggregate(MP, plus)

            M1 = [(m, 1) for ((m, p, d), (p2, d2)) in product(MPD, PD) if p == p2 and d == d2]
            MC = aggregate(M1, sum)

            M = [scale(t, c) for ((m, t), (m2, c)) in product(MT, MC) if m == m2]
            print(sorted(M))

            # first it only returned one point, second the point it returned is in the sea




        # Create a new collection and insert the result data set
        repo.dropPermanent("k_means")
        repo.createPermanent("k_means")


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}





    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        '''
        '''
        # The agent
        this_script = doc.agent('alg:aydenbu_huangyh#merge_school_garden',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # The source entity
        school_source = doc.entity('dat:public_school_count',
                              {'prov:label': 'Public School Count', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        garden_source = doc.entity('dat:community_garden_count',
                                  {'prov:label': 'Community Garden Count', prov.model.PROV_TYPE: 'ont:DataResource',
                                   'ont:Extension': 'json'})

        # The activity
        get_zip_public = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "Merge the numbers of garden and school in each zip"})

        # The activity is associated with the agent
        doc.wasAssociatedWith(get_zip_public, this_script)

        # Usage of the activity: Source Entity
        doc.usage(get_zip_public, school_source, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_zip_public, garden_source, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        # The Result Entity
        zip_public = doc.entity('dat:aydenbu_huangyh#merge_school_garden',
                                            {prov.model.PROV_LABEL: 'Merge Public Building Count',
                                             prov.model.PROV_TYPE: 'ont:DataSet'})

        # Result Entity was attributed to the agent
        doc.wasAttributedTo(zip_public, this_script)

        # Result Entity was generated by the activity
        doc.wasGeneratedBy(zip_public, get_zip_public, endTime)

        # Result Entity was Derived From Source Entity
        doc.wasDerivedFrom(zip_public, school_source, get_zip_public, get_zip_public,
                           get_zip_public)
        doc.wasDerivedFrom(zip_public, garden_source, get_zip_public, get_zip_public,
                           get_zip_public)

        repo.record(doc.serialize())
        repo.logout()

        return doc

k_means.execute()
doc = k_means.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))