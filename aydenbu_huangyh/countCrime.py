import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *



class countPublicSchool(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.crime_zip']
    writes = ['aydenbu_huangyh.zip_crime_count']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection

        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        crimes = repo['aydenbu_huangyh.crime_zip']

        crimes_array = []
        for document in crimes.find():
            crimes_array.append({"zip": document['zip'][1:], 'num': document['num']})

        repo.dropPermanent("test")
        repo.createPermanent("test")
        repo['aydenbu_huangyh.test'].insert_many(crimes_array)

        test = repo['aydenbu_huangyh.test']

        # MapReduce function
        mapper = Code("""
                        function() {
                            emit(this.zip, {numofCrime:1});
                        }
                      """)
        reducer = Code("""
                        function(k, vs) {
                            var total = 0;
                            for (var i = 0; i < vs.length; i++) {
                                total += vs[i].numofCrime;
                            }
                            return  { numofCrime: total};
                        }"""
                      )
        repo.dropPermanent("zip_crime_count")
        result = test.map_reduce(mapper, reducer, "aydenbu_huangyh.zip_crime_count")

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

        this_script = doc.agent('alg:aydenbu_huangyh#countCrime',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource = doc.entity('dat:crime_zip',
                              {'prov:label': 'crime Zipcode', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        get_zip_crime_count = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "Count the number of Crimes in each zip"})
        doc.wasAssociatedWith(get_zip_crime_count, this_script)
        doc.usage(get_zip_crime_count, resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        zip_crime_count = doc.entity('dat:aydenbu_huangyh#zip_crime_count',
                                         {prov.model.PROV_LABEL: 'Crimes Count',
                                          prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(zip_crime_count, this_script)
        doc.wasGeneratedBy(zip_crime_count, get_zip_crime_count, endTime)
        doc.wasDerivedFrom(zip_crime_count, resource, get_zip_crime_count, get_zip_crime_count,
                           get_zip_crime_count)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


countPublicSchool.execute()
doc = countPublicSchool.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## add provenance
## eof
