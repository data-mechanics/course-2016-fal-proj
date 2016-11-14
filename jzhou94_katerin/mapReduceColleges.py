import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code


class mapReduceColleges(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = ['jzhou94_katerin.colleges']
    writes = ['jzhou94_katerin.mapColleges']
        
    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')
 
        map_function_college = Code('''function() {
            emit(this.City, {colleges:1});
            }''')
        
        reduce_function_college = Code('''function(k, vs) {
            var total = 0;
            for (var i = 0; i < vs.length; i++)
                total += vs[i].colleges;
            return {colleges:total};
            }''')
        
        repo.dropPermanent('jzhou94_katerin.mapColleges')
        repo.createPermanent('jzhou94_katerin.mapColleges')
        repo.jzhou94_katerin.colleges.map_reduce(map_function_college, reduce_function_college, 'jzhou94_katerin.mapColleges');

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

        this_script = doc.agent('alg:mapReduceSchools', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        public_schools = doc.entity('dat:public_schools', {prov.model.PROV_LABEL:'Public Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_schools, this_script)
        doc.usage(get_schools, public_schools, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )

        schools = doc.entity('dat:schools', {prov.model.PROV_LABEL:'Number of Schools in Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, public_schools, get_schools, get_schools, get_schools)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

mapReduceColleges.execute()
print("mapReduceSchools Algorithm Done")
doc = mapReduceColleges.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("mapReduceColleges Provenance Done")

## eof
