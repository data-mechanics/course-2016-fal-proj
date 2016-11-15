import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code


class mapReduceAvgEarnings(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = ['jzhou94_katerin.employee_earnings']
    writes = ['jzhou94_katerin.avg_earnings']
        
    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        map_function_avg_earnings = Code('''function() {
            if (this.postal*1 > 2100 && this.postal*1 < 2300 && this.title == "Police Officer")
            emit(this.postal, {tot:this.total_earnings, n: 1, avg: this.total_earnings});
            }''')
        
        reduce_function_avg_earnings = Code('''function(k, vs) {            
            var total = 0;
            var counts = 0;
            for (var i = 0; i < vs.length; i++)
            total += (vs[i].tot*1);
            for (var i = 0; i < vs.length; i++)
            counts += vs[i].n;
            
            return {tot:total.toFixed(2), n: counts, avg: (total/counts).toFixed(2)};
            }''')
        
        if (trial):
            print('adfasdfasfsa')

        repo.dropPermanent('jzhou94_katerin.avg_earnings')
        repo.createPermanent('jzhou94_katerin.avg_earnings')

        if trial == True:
            repo['jzhou94_katerin.avg_earnings'].insert(repo.jzhou94_katerin.employee_earnings.find().limit(100))
            repo.jzhou94_katerin.avg_earnings.map_reduce(map_function_avg_earnings, reduce_function_avg_earnings, 'jzhou94_katerin.avg_earnings');
        else:
            repo.jzhou94_katerin.employee_earnings.map_reduce(map_function_avg_earnings, reduce_function_avg_earnings, 'jzhou94_katerin.avg_earnings');

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

        this_script = doc.agent('alg:mapReduceAvgEarnings', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        employee_earnings = doc.entity('dat:employee_earnings', {prov.model.PROV_LABEL:'Employee Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
        get_avg_earnings = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_avg_earnings, this_script)
        doc.usage(get_avg_earnings, employee_earnings, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )

        avg_earnings = doc.entity('dat:avg_earnings', {'prov:label':'Average Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(avg_earnings, this_script)
        doc.wasGeneratedBy(avg_earnings, get_avg_earnings, endTime)
        doc.wasDerivedFrom(avg_earnings, employee_earnings, get_avg_earnings, get_avg_earnings, get_avg_earnings)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

mapReduceAvgEarnings.execute()
print("mapReduceAvgEarnings Algorithm Done")
doc = mapReduceAvgEarnings.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("mapReduceAvgEarnings Provenance Done")

## eof
