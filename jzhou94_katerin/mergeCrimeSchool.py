import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code


class mergeCrimeSchool(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = ['jzhou94_katerin.schools', 'jzhou94_katerin.crime']
    writes = ['jzhou94_katerin.merge']
        
    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        S = [doc for doc in repo.jzhou94_katerin.schools.find()]
 
        C = [doc for doc in repo.jzhou94_katerin.crime.find()]

        """
        MERGE
        """
        repo.dropPermanent("merge")
        repo.createPermanent("merge")
        def product(R, S):
            return [(t, u) for t in R for u in S if(t['_id'] == u['_id'])]
        P = product(S, C)
        j = 0
        for i in P:
            repo['jzhou94_katerin.merge'].insert({'Name': P[j]})
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

        this_script = doc.agent('alg:mergeCrimeSchool', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        schools = doc.entity('dat:schools', {prov.model.PROV_LABEL:'Number of Schools in Location', prov.model.PROV_TYPE:'ont:DataSet'})
        crime = doc.entity('dat:crime', {prov.model.PROV_LABEL:'Crimes per Location', prov.model.PROV_TYPE:'ont:DataSet'})
        get_merge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_merge, this_script)
        doc.usage(get_merge, schools, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )
        doc.usage(get_merge, crime, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )

        merge = doc.entity('dat:merge', {prov.model.PROV_LABEL:'Crimes to Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(merge, this_script)
        doc.wasGeneratedBy(merge, get_merge, endTime)
        doc.wasDerivedFrom(merge, schools, get_merge, get_merge, get_merge)
        doc.wasDerivedFrom(merge, crime, get_merge, get_merge, get_merge)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

mergeCrimeSchool.execute()
print("mergeCrimeSchool Algorithm Done")
doc = mergeCrimeSchool.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("mergeCrimeSchool Provenance Done")

## eof
