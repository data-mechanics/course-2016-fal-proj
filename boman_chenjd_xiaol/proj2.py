import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import re
import random as ra
import numpy as np
import math
from scipy import stats
from scipy.stats import normaltest


class proj2(dml.Algorithm):
    contributor='boman_chenjd_xiaol'
    reads=[]
    writes=['boman_chenjd_xiaol.crime_report','boman_chenjd_xiaol.property_assessment','boman_chenjd_xiaol.community_garden','boman_chenjd_xiaol.liquor','boman_chenjd_xiaol.school_garden']
    @staticmethod
    def execute(trial=False):
        print ("start data retrieval:")
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('boman_chenjd_xiaol', 'boman_chenjd_xiaol')

        startTime=datetime.datetime.now()
        url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("crime_report")
        repo.createPermanent("crime_report")
        repo['boman_chenjd_xiaol.crime_report'].insert_many(r)

        url = 'https://data.cityofboston.gov/resource/g5b5-xrwi.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("property_assessment")
        repo.createPermanent("property_assessment")
        repo['boman_chenjd_xiaol.property_assessment'].insert_many(r)

        url = 'https://data.cityofboston.gov/resource/rdqf-ter7.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("community_garden")
        repo.createPermanent("community_garden")
        repo['boman_chenjd_xiaol.community_garden'].insert_many(r)

        url='https://data.cityofboston.gov/resource/g9d9-7sj6.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("liquor")
        repo.createPermanent("liquor")
        repo['boman_chenjd_xiaol.liquor'].insert_many(r)

        url='https://data.cityofboston.gov/resource/pzcy-jpz4.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("school_garden")
        repo.createPermanent("school_garden")
        repo['boman_chenjd_xiaol.school_garden'].insert_many(r)


        crime_report=repo['boman_chenjd_xiaol.crime_report']
        property_assessment=repo['boman_chenjd_xiaol.property_assessment']
        community_garden=repo['boman_chenjd_xiaol.community_garden']
        liquor=repo['boman_chenjd_xiaol.liquor']
        school_garden=repo['boman_chenjd_xiaol.school_garden']



        
        def closest(R,S):
            shortest=[]
            for i in range(len(R)):
                dist2=[]
                for j in range(len(S)):
                    dist1=distance(R[i],S[j])
                    dist2.append(dist1)
                shortest.append(min(dist2))
            return shortest


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
            return [(key, f([v for (k,v) in R if k == key] + [])) for key in keys]
            
        def map(f, R):
            return [t for (k,v) in R for t in f(k,v)]
            
        def reduce(f, R):
            keys = {k for (k,v) in R}
            return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

        def intersect(R,S):
            return [t for t in r if t in S]

        def getCollection(db):
            temp=[]
            for i in repo['boman_chenjd_xiaol.'+db].find():
                temp.append(i)
            return temp

        x=getCollection('liquor')
        X=[]
        for i in range(len(x)):
            X.append(x[i]['location'])
        #print (X)
        temp_x=[]
        for item in X:
            for k,v in item.items():
                if k=='coordinates':
                    temp_x.append(v)
        liquor_test=[(i,j) for [i,j] in temp_x]


        y=getCollection('school_garden')
        Y=[]
        for i in range(len(y)):
            Y.append(y[i]['coordinates'])

        z=getCollection('community_garden')

        Z=[]
        count=0
        for i in range(len(z)):
            try:
                Z.append(z[i]['coordinates'])
            except Exception as e:
                print (e)


        t=getCollection('property_assessment')
        LA=[]
        LO=[]
        gross_tax=[]
        T=[]
        for i in range(len(t)):
            LO.append(t[i]['longitude'])
            LA.append(t[i]['latitude'])
            gross_tax.append(t[i]['gross_tax'])
        for j in range(len(LA)):
            T.append((LO[j],LA[j],int(gross_tax[j])))
        int_z=[int(i) for i in gross_tax]
        print (int_z)
        while 0 in int_z:
            int_z.remove(0)
        z,pval=normaltest(int_z)
        print ("This is the p-value")
        print (pval)
        '''
        if (pval<0.055):
            print ("This is not normal distribution")
        else:
            print ("It is a normal distribution")'''
        temp_zscore=stats.zscore(int_z)
        remain_list=[]
        for i in range(len(temp_zscore)):
            if temp_zscore[i]>=1.28: # upper 10%
                remain_list.append(i)
        richlist=[]
        for i in remain_list:
            richlist.append(T[i])
        #print ("this is the rich list:")
        #print (len(richlist))
        #print (richlist)


        #print ("This is the property_assessment tuple:")

        #print (T)



        c=getCollection('crime_report')
        C=[]
        for i in range(len(c)):
            C.append(c[i]['location'])

        temp_c=[]
        for item in C:
            for k,v in item.items():
                if k=='coordinates':
                    temp_c.append(v)

        # while [0,0] in temp_c:
        #     temp_c.remove([0,0])
        crime_test=[(i,j) for [i,j] in temp_c]



        X_factor_garden=union(Z,Y)
        while 'latitude,longitude' in X_factor_garden:
            X_factor_garden.remove('latitude,longitude')

        #print (X_factor_garden)
        count=10
        X_random=[]
        while count>0:
            a=ra.uniform(42.232361,42.393294)#get from Boston_min_la,Boston_max_la,Boston_min_lo,Boston_max_lo
            b=ra.uniform(-71.17312,-71.001859)
            X_random.append((a,b))
            count-=1

        def distance(origin, destination):
            lat1, lon1 = origin
            lat2, lon2 = destination
            radius = 6371 # km

            dlat = math.radians(lat2-lat1)
            dlon = math.radians(lon2-lon1)
            a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
                * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
            c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
            d = radius * c

            return d

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

        M = X_random

        P = [eval(x) for x in X_factor_garden]
        def K_means(T,P):
            OLD = []
            M=T
            counter=1
            while OLD != M:
                OLD = M

                MPD = [(m, p, dist(m,p)) for [m, p] in product(M, P)]
                PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
                PD = aggregate(PDs, min)
                MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
                MT = aggregate(MP, plus)

                M1 = [(m, 1) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
                MC = aggregate(M1, sum)

                M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]
                M=sorted(M)
                counter+=1
                print (M)
                print (counter)
            return M
        #print (P)
        #print (len(P))
        garden_cluster=[]
        K=K_means(M,P)
        print ("K-means of garden finished")
        for (i,j) in K:
            garden_cluster.append({'location':(i,j)})
        repo.dropPermanent("c_garden")
        repo.createPermanent("c_garden")
        repo['boman_chenjd_xiaol.c_garden'].insert_many(garden_cluster)
        #print ("Successfully import data to repo")
        #print (len(crime_test))
        #crime=[(i,j) for (i,j) in ra.sample(crime_test,2000) if (i,j)!=(0,0)]
        crime=[(j,i) for (i,j) in crime_test if (i,j)!=(0,0)]
        # Bos_min_l1=min([i for (i,j) in crime])
        # Bos_max_l1=max([i for (i,j) in crime])
        # Bos_max_l2=max([j for (i,j) in crime])
        # Bos_min_l2=min([j for (i,j) in crime])
        #print (Bos_min_l1,Bos_min_l2,Bos_max_l1,Bos_max_l2)
        print (len(crime))
        #Temp=K_means(X_random,crime)

        #print ("K-means of crime locations done")
        Crime_cluster=[]
        #for (i,j) in Temp:
        for (i,j) in crime:
            Crime_cluster.append({'location':(i,j)})
        print (" crime done")
        liquor_tuple=[(j,i) for (i,j) in liquor_test if (i,j)!=(0,0)]

        #liquor_storage=K_means(X_random,liquor_tuple)
        liquor_cluster=[]
        #for (i,j) in liquor_storage:
        for (i,j) in liquor_tuple:
            liquor_cluster.append({'location':(i,j)})
        print ("liquor done")
        #print (K)
        #print (crime)
    
        from_garden_to_crime=closest(P,crime)
        std_garden_to_crime=np.std(from_garden_to_crime)
        from_crime_to_garden=closest(crime, P)
        std_crime_to_garden=np.std(from_crime_to_garden)

        #print (from_crime_to_garden)
        #print (from_garden_to_crime)
        print ("This is the standard deviation between garden and crime:")
        print (std_crime_to_garden)
        print (std_garden_to_crime)
        #print (liquor_tuple)
        #print (crime)
        from_liquor_to_crime=closest(liquor_tuple,crime)
        std_liquor_to_crime=np.std(from_liquor_to_crime)
        from_crime_to_liquor=closest(crime, liquor_tuple)
        std_crime_to_liquor=np.std(from_crime_to_liquor)
        print ("This is the standard deviation between liquor and crime:")
        print (std_crime_to_liquor)
        print (std_liquor_to_crime)

        richlist_test=[(float(y),float(x)) for (x,y,z) in richlist if (x,y)!=('#N/A','#N/A')]
        richlist_cluster=[]
        for (i,j,k) in richlist:
            richlist_cluster.append({'location':(i,j)})
        #print (richlist_test)
        #print (crime)
        from_richlist_to_crime=closest(richlist_test,crime)
        std_richlist_to_crime=np.std(from_richlist_to_crime)
        from_crime_to_richlist=closest(crime, richlist_test)
        std_crime_to_richlist=np.std(from_crime_to_richlist)
        print ("This is the standard deviation between richlist and crime:")
        print (std_crime_to_richlist)
        print (std_richlist_to_crime)


        repo.dropPermanent("richlist")
        repo.createPermanent("richlist")
        repo['boman_chenjd_xiaol.richlist'].insert_many(richlist_cluster)

        repo.dropPermanent("crime_cluster")
        repo.createPermanent("crime_cluster")
        repo['boman_chenjd_xiaol.crime_cluster'].insert_many(Crime_cluster)

        repo.dropPermanent("liquor_cluster1")
        repo.createPermanent("liquor_cluster1")
        repo['boman_chenjd_xiaol.liquor_cluster1'].insert_many(liquor_cluster)
        endTime=datetime.datetime.now()
        return {"start":startTime, "end":endTime}
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('boman_chenjd_xiaol', 'boman_chenjd_xiaol')
        doc=prov.model.ProvDocument()
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/boman_chenjd_xiaol/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/boman_chenjd_xiaol/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:proj2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        c_g = doc.entity('bdp:cr3i-jj7v', {'prov:label':'community garden', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        s_g = doc.entity('bdp:cxb7-aa9j', {'prov:label':'school garden', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        ll = doc.entity('bdp:hda6-fnsh', {'prov:label':'liquor license', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        c_i = doc.entity('bdp:ufcx-3fdn', {'prov:label':'crime incident', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        pro = doc.entity('bdp:g5b5-xrwi', {'prov:label':'property', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        c_garden= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        crime_cluster = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        liquor_cluster1= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        richlist=doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(c_garden, this_script)
        doc.wasAssociatedWith(crime_cluster, this_script)
        doc.wasAssociatedWith(liquor_cluster1, this_script)
        doc.wasAssociatedWith(richlist, this_script)

        c_garden = doc.entity('dat:c_garden', {prov.model.PROV_LABEL:'c_garden', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(c_garden, this_script)
        doc.wasGeneratedBy(c_garden, c_garden, endTime)
        doc.wasDerivedFrom(c_garden, c_g, c_garden, c_garden, c_garden)

        crime_cluster = doc.entity('dat:crime_cluster', {prov.model.PROV_LABEL:'crime_cluster', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_cluster, this_script)
        doc.wasGeneratedBy(crime_cluster, crime_cluster, endTime)
        doc.wasDerivedFrom(crime_cluster, c_i, crime_cluster, crime_cluster, crime_cluster)

        liquor_cluster1 = doc.entity('dat:liquor_cluster1', {prov.model.PROV_LABEL:'liquor_cluster1', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(liquor_cluster1, this_script)
        doc.wasGeneratedBy(liquor_cluster1, liquor_cluster1, endTime)
        doc.wasDerivedFrom(liquor_cluster1, ll, liquor_cluster1, liquor_cluster1, liquor_cluster1)

        richlist_cluster = doc.entity('dat:richlist', {prov.model.PROV_LABEL:'richlist', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(richlist_cluster, this_script)
        doc.wasGeneratedBy(richlist_cluster, richlist_cluster, endTime)
        doc.wasDerivedFrom(richlist_cluster,pro)
        
        doc.used(c_garden, c_g, startTime)
        doc.used(crime_cluster, c_i, startTime)
        doc.used(liquor_cluster1, ll, startTime)


        repo.record(doc.serialize()) # Record the provenance document.
        open('plan.json','w').write(json.dumps(json.loads(doc.serialize()), indent=4))
        print(doc.get_provn())
        repo.logout()
        return doc
proj2.execute()
doc=proj2.provenance()


        
