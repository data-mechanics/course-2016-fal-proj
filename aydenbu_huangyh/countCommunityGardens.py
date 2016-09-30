import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps



class countCommunityGardens(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.communityGardens']
    writes = ['aydenbu_huangyh.zip_communityGardens_count']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aydenbu_huangyh', 'aydenbu_huangyh')
        communityGardens = repo['aydenbu_huangyh.communityGardens']

        # MapReduce function
        mapper = Code("""
                        function() {
                            emit(this.zip_code, 1);
                        }
                        """)
        reducer = Code("""
                        function(k, vs) {
                            var total = 0;
                            for (var i = 0; i < vs.length; i++) {
                                total += vs[i];
                            }
                            return total;
                        }"""
                       )
        repo.dropPermanent("zip_communityGardens_count")

        result = communityGardens.map_reduce(mapper, reducer, "aydenbu_huangyh.zip_communityGardens_count")

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

        this_script = doc.agent('alg:aydenbu_huangyh#countCommunityGardens',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:community_gardens',
                              {'prov:label': 'Community Gardens', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_zip_communityGarden_count = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "Count the number of CommunityGarden in each zip"})
        doc.wasAssociatedWith(get_zip_communityGarden_count, this_script)
        doc.usage(get_zip_communityGarden_count, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        zip_communityGarden_count = doc.entity('dat:aydenbu_huangyh#zip_communityGarden_count',
                                         {prov.model.PROV_LABEL: 'CommunityGarden Count',
                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(zip_communityGarden_count, this_script)
        doc.wasGeneratedBy(zip_communityGarden_count, get_zip_communityGarden_count, endTime)
        doc.wasDerivedFrom(zip_communityGarden_count, resource, get_zip_communityGarden_count, get_zip_communityGarden_count,
                           get_zip_communityGarden_count)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc



countCommunityGardens.execute()
doc = countCommunityGardens.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## add provenance
## eof

