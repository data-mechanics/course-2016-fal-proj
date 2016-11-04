#schoolsPerNeighborhood.py
# Learned how to use map reduce function from stackoverflow question below
#http://stackoverflow.com/questions/23643327/mongodb-aggregate-group-array-to-key-sum-value

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class schoolsPerNeighborhood(dml.Algorithm):
    contributor = 'jyaang_robinliu106'
    reads = ['jyaang_robinliu106.school']
    writes = ['jyaang_robinliu106.schoolsPerNeighborhood']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        school_List = repo['jyaang_robinliu106.school']

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

        repo.dropPermanent("schoolsPerNeighborhood")
        repo.createPermanent("schoolsPerNeighborhood")
        repo.jyaang_robinliu106.school.map_reduce(cityMap,r, "jyaang_robinliu106.schoolsPerNeighborhood")

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

        this_script = doc.agent('alg:jyaang_robinliu106#schoolsPerNeighborhood', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'Neighborhoods schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_schoolsPerNeighborhood = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_schoolsPerNeighborhood, this_script)
        doc.usage(get_schoolsPerNeighborhood, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=school&$select=_id,value,'
                }
            )

        school = doc.entity('dat:jyaang_robinliu106#school', {prov.model.PROV_LABEL:'school Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(school, this_script)
        doc.wasGeneratedBy(school, get_schoolsPerNeighborhood, endTime)
        doc.wasDerivedFrom(school, resource, get_schoolsPerNeighborhood, get_schoolsPerNeighborhood, get_schoolsPerNeighborhood)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc


schoolsPerNeighborhood.execute()
doc = schoolsPerNeighborhood.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
