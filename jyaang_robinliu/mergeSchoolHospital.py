#hospitalsPerNeighborhood.py
# Learned how to use map reduce function from stackoverflow question below
#http://stackoverflow.com/questions/23643327/mongodb-aggregate-group-array-to-key-sum-value

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class mergeSchoolHospital(dml.Algorithm):
    contributor = 'jyaang_robinliu106'
    reads = ['jyaang_robinliu106.hospital','jyaang_robinliu106.school']
    writes = ['jyaang_robinliu106.mergeSchoolHospital']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        cityMap = Code('''function () {
                            var cityList = {};
                            var name = this.name;
                            cityList[name] = 1;

                            emit( this.city, cityList );
                            }

                        ''')

        r = Code('''function (key,values) {

                                        var cityList = {};

                                        values.forEach(function(value) {
                                            for ( var city in value ) {
                                                if ( !cityList.hasOwnProperty(city) )
                                                    cityList[city] = 0;
                                                cityList[city]++;
                                            }
                                        });

                                        return cityList;
                                    };

                                ''')

        repo.jyaang_robinliu106.mergeSchoolHospital.drop()

        for record in repo.jyaang_robinliu106.school.find():
            repo.jyaang_robinliu106.mergeSchoolHospital.insert(record,check_keys=False)

        for record in repo.jyaang_robinliu106.hospital.find():
            repo.jyaang_robinliu106.mergeSchoolHospital.insert(record,check_keys=False)

        repo.jyaang_robinliu106.mergeSchoolHospital.map_reduce(cityMap,r,'jyaang_robinliu106.mergeSchoolHospital')

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
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jyaang_robinliu106#mergeSchoolHospital', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Schools & Hospitals Per Neighborhood', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        getSchool = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        getHospital = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(getSchool, this_script)
        doc.wasAssociatedWith(getHospital, this_script)
        doc.usage(getSchool, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=getSchool&$select=name,city,coord'
                }
            )
        doc.usage(getHospital, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=getHospital&$select=name,city,coord'
                }
            )

        mergeSchoolHospital = doc.entity('dat:jyaang_robinliu106#mergeSchoolHospital', {prov.model.PROV_LABEL:'merge Day Camp Hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mergeSchoolHospital, this_script)
        doc.wasGeneratedBy(mergeSchoolHospital, getHospital, endTime)
        doc.wasDerivedFrom(mergeSchoolHospital, resource, getHospital, getHospital, getHospital)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


mergeSchoolHospital.execute()
doc = mergeSchoolHospital.provenance()
print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
