import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps



class zipAverageEarning(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = []
    writes = ['aydenbu_huangyh.earningsReport']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Setup the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aydenbu_huangyh', 'aydenbu_huangyh')
        earnings = repo['aydenbu_huangyh.earningsReport']

        # MaoReduce function
        mapper = Code(
            """
            function() {
                var k = this.postal;
                var v = {count:1, totalEarnings:parseFloat(this.total_earnings)};
                emit(k, v)
            }
            """
        )

        reducer = Code(
            """
            function(k, vs) {
                reduceVal = {count:0, totalEarnings:0, avg:0};
                for (var i = 0; i < vs.length; i++) {
                    reduceVal.count += vs[i].count;
                    reduceVal.totalEarnings += parseFloat(vs[i].totalEarnings);
                }
                reduceVal.avg = reduceVal.totalEarnings/reduceVal.count;
                return reduceVal;
            }
            """
        )

        repo.dropPermanent("zip_avg_earnings")
        result = earnings.map_reduce(mapper, reducer, "aydenbu_huangyh.zip_avg_earnings")

        '''
                # Save the result to the db
                # repo.dropPermanent("zip_hospitals_count")
                #repo.createPermanent("zip_hospitals_count")
                # repo['aydenbu_huangyh.zip_hospitals_count'].insert_many(result)
                '''
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
        repo.authenticate('aydenbu_huangyh', 'aydenbu_huangyh')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aydenbu_huangyh#zip_avg_earnings',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('bdp:wc8w-nujj',
                              {'prov:label': '311, Service Requests', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_zip_avg_earnings = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_zip_avg_earnings, this_script)
        doc.usage(get_zip_avg_earnings, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        zip_avg_earnings = doc.entity('dat:aydenbu_huangyh#zip_avg_earnings',
                                         {prov.model.PROV_LABEL: 'Avg Earnings',
                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(zip_avg_earnings, this_script)
        doc.wasGeneratedBy(zip_avg_earnings, get_zip_avg_earnings, endTime)
        doc.wasDerivedFrom(zip_avg_earnings, resource, get_zip_avg_earnings, get_zip_avg_earnings,
                           get_zip_avg_earnings)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


zipAverageEarning.execute()
doc = zipAverageEarning.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## add provenance
## eof