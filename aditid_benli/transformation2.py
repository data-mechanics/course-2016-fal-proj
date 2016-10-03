import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class transformation2(dml.Algorithm):
    contributor = 'aditid_benli'
    reads = ['aditid_benli.jam', 'aditid_benli.crime']
    writes = ['aditid_benli.crimeLocations','aditid_benli.jamMR']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli', 'aditid_benli')
        

        #Put Crime and locations together
        map_function = Code('''function() {
            if (this.street != undefined)
                {
                if (this.lat != undefined)
                    {
                    id = {
                        street:this.street,
                        lat:this.lat,
                        long:this.long
                        }
                    emit(id,num:1);
                    }
                }
            }''')


        reduce_function = Code('''function(k, vs) {
            var total = 0;
            for (var i = 0; i < vs.length; i++)
            total += vs[i].num;
            return {num:total};
            }''')
        
        #reset resulting directory
        repo.dropPermanent('aditid_benli.crimeLocations')
        repo.createPermanent('aditid_benli.crimeLocations')

        repo.aditid_benli.crime.map_reduce(map_function, reduce_function, 'aditid_benli.crimeLocations');


        #union
        def union(collection1, collection2, result):
            for document in repo[collection1].find():
                repo[result].insert(document)
                
            for document in repo[collection2].find():
                repo[result].insert(document)
        
        #reset resulting directory
        repo.dropPermanent('aditid_benli.jamCrime')
        repo.createPermanent('aditid_benli.jamCrime')
        
        
#        union('aditid_benli.jam', 'aditid_benli.crime','aditid_benli.jamCrime')
#
#        #Find coordinates of each street
#        map_function = Code('''function() {
#            if (this._id == this.street){
#                emit(this._id, {
#            
#            emit(this.street, {num:1});
#            }''')
#        
#        
#        +            id = this._id.zip_code;
#            +            if (this._id.type == 'sr311') {
#                +                emit(id, {crime: 0, sr311: this.value});
#                    +            } else {
#                        +                emit(id, {crime: this.value, sr311: 0});
#                            +            }
#                                +        }''')
#
#        
#        reduce_function = Code('''function(k, vs) {
#            var total = 0;
#            for (var i = 0; i < vs.length; i++)
#            total += vs[i].num;
#            return {jams:total};
#            }''')
#        

        repo.aditid_benli.jamCrime.map_reduce(map_function, reduce_function, 'aditid_benli.jamMR');
        

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
        repo.authenticate('alice_bob', 'alice_bob')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                }
            )
        doc.usage(get_lost, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                }
            )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

transformation2.execute()
doc = transformation2.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof