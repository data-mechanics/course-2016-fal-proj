import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from random import shuffle
from math import sqrt


class p_value(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = ['jzhou94_katerin.crime', 'jzhou94_katerin.avg_earnings']
    writes = ['jzhou94_katerin.p_value']
        
    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        avg_earnings = [doc for doc in repo.jzhou94_katerin.avg_earnings.find()]
        crimes = [doc for doc in repo.jzhou94_katerin.crime.find()]

        # tuple (zipcode, avg_earning)
        zip_avg = [(item['_id'], float(item['value']['avg'])) for item in avg_earnings]
        # tuple (zipcode, number_crimes)
        zip_crimes = [(item['_id'], item['value']['crime']) for item in crimes]

        data = [(x2, y2) for (x1, x2) in zip_avg for (y1, y2) in zip_crimes if x1 == y1]
        
        x = [xi for (xi, yi) in data]
        y = [yi for (xi, yi) in data]
        
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
            return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

        def corr(x, y): # Correlation coefficient.
            if stddev(x)*stddev(y) != 0:
                return cov(x, y)/(stddev(x)*stddev(y))

        def p(x, y):
            c0 = corr(x, y)
            corrs = []
            for k in range(0, 2000):
                y_permuted = permute(y)
                corrs.append(corr(x, y_permuted))
            return len([c for c in corrs if abs(c) > c0])/len(corrs)

        repo.dropPermanent('jzhou94_katerin.p_value')
        repo.createPermanent('jzhou94_katerin.p_value')

        repo['jzhou94_katerin.p_value'].insert(p(x, y))

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

        this_script = doc.agent('alg:p_value', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        crime = doc.entity('dat:crime', {prov.model.PROV_LABEL:'Crimes per Location', prov.model.PROV_TYPE:'ont:DataSet'})
        avg_earnings = doc.entity('dat:avg_earnings', {'prov:label':'Average Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
        get_pValue = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_pValue, this_script)
        doc.usage(get_pValue, crime, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )
        doc.usage(get_pValue, avg_earnings, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )

        pValue = doc.entity('dat:p_value', {prov.model.PROV_LABEL:'P Value of Crimes to Average Earnings', prov.model.PROV_TYPE:'ont:Value'})
        doc.wasAttributedTo(pValue, this_script)
        doc.wasGeneratedBy(pValue, get_pValue, endTime)
        doc.wasDerivedFrom(pValue, crime, get_pValue, get_pValue, get_pValue)
        doc.wasDerivedFrom(pValue, avg_earnings, get_pValue, get_pValue, get_pValue)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

p_value.execute()
print("p_value Algorithm Done")
doc = p_value.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("p_value Provenance Done")
## eof
