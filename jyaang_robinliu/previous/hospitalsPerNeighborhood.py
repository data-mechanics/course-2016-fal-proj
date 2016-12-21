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

class hospitalsPerNeighborhood(dml.Algorithm):
    contributor = 'jyaang_robinliu106'
    reads = ['jyaang_robinliu106.hospital']
    writes = ['jyaang_robinliu106.hospitalsPerNeighborhood']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        hospital_List = repo['jyaang_robinliu106.hospital']

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

        repo.dropPermanent("hospitalsPerNeighborhood")
        repo.createPermanent("hospitalsPerNeighborhood")
        repo.jyaang_robinliu106.hospital.map_reduce(cityMap,r, "jyaang_robinliu106.hospitalsPerNeighborhood")

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

        this_script = doc.agent('alg:jyaang_robinliu106#hospitalsPerNeighborhood', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Neighborhoods Hospitals', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_hospitalsPerNeighborhood = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hospitalsPerNeighborhood, this_script)
        doc.usage(get_hospitalsPerNeighborhood, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=hospital&$select=_id,value,'
                }
            )

        hospital = doc.entity('dat:jyaang_robinliu106#hospital', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospital, this_script)
        doc.wasGeneratedBy(hospital, get_hospitalsPerNeighborhood, endTime)
        doc.wasDerivedFrom(hospital, resource, get_hospitalsPerNeighborhood, get_hospitalsPerNeighborhood, get_hospitalsPerNeighborhood)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


hospitalsPerNeighborhood.execute()
doc = hospitalsPerNeighborhood.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
