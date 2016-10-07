import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class transformation1(dml.Algorithm):
    contributor = 'aditid_benli'
    reads = ['aditid_benli.jam', 'aditid_benli.crime']
    writes = ['aditid_benli.jamMR','aditid_benli.crimeLocations']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli', 'aditid_benli')
        
        
        #Find number of jams per street and put in jamMR
        map_function = Code('''function() {
            emit(this.street, {jams:1});
            }''')
        
    
        reduce_function = Code('''function(k, vs) {
            var total = 0;
            for (var i = 0; i < vs.length; i++)
            total += vs[i].jams;
            return {jams:total};
            }''')

        #reset resulting directory
        repo.dropPermanent('aditid_benli.jamMR')
        repo.createPermanent('aditid_benli.jamMR')
        
        repo.aditid_benli.jam.map_reduce(map_function, reduce_function, 'aditid_benli.jamMR');


        #get the number of crimes per street
        #also average latitute and longitude of the street
        #put in crimeLocations
        map_function = Code('''function() {
            if (this.street != undefined)
                {
                if (this.lat != undefined)
                    {
                    id = {
                        street:this.street,
                    }
                    emit(id,{lat:this.lat,long:this.long,crime:1});
                    }
                }
            }''')
        
        
        reduce_function = Code('''function(k, vs) {
            var total = 0;
            var sum = 0;
            var tlat = 0;
            var tlong = 0;

            for (var i = 0; i < vs.length; i++)
                {
                total += vs[i].crime
                tlat += parseFloat(vs[i].lat)
                tlong += parseFloat(vs[i].long)
                sum = sum + 1
                }
            mlat = parseFloat(tlat) / sum
            mlong = parseFloat(tlong) / sum
            return {lat:mlat,long:mlong,crime:total, sum:sum};
            }''')
        
        #reset resulting directory
        repo.dropPermanent('aditid_benli.crimeLocations')
        repo.createPermanent('aditid_benli.crimeLocations')
        
        repo.aditid_benli.crime.map_reduce(map_function, reduce_function, 'aditid_benli.crimeLocations');
        
        #join jamMR and crimeLocations and put in jamCrim
        #jamCrime contains number of jams and number of crimes per street
        def join(collection1, collection2, result):
            for document1 in repo[collection1].find():
                doc1 = str(document1['_id'])
                for document2 in repo[collection2].find():
                    doc2 = str(document2['_id']['street'])
                    if doc1.lower() == doc2.lower():
                        document1['value']['long'] = document2['value']['long']
                        document1['value']['lat'] = document2['value']['lat']
                        document1['value']['crime'] = document2['value']['crime']
                        repo[result].insert(document1)



        #reset resulting directory
        repo.dropPermanent('aditid_benli.jamCrime')
        repo.createPermanent('aditid_benli.jamCrime')
        
        join('aditid_benli.jamMR', 'aditid_benli.crimeLocations', 'aditid_benli.jamCrime');

        #end
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
        repo.authenticate('aditid_benli', 'aditid_benli')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aditid_benli#transformation1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_script = doc.agent('alg:aditid_benli#transformation0', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        jamMR = doc.entity('dat:aditid_benli#jamMR', {prov.model.PROV_LABEL:'Collide metered parking with jams', prov.model.PROV_TYPE:'ont:DataSet'})        
        getjamMR = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Collide metered parking with map reduce'})        
        doc.wasAssociatedWith(getjamMR, this_script)
        doc.used(getjamMR, jamMR, startTime)
        doc.wasAttributedTo(jamMR, this_script)
        doc.wasGeneratedBy(jamMR, getjamMR, endTime)


        this_script = doc.agent('alg:aditid_benli#transformation1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_script = doc.agent('alg:aditid_benli#transformation0', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        crimeLocations = doc.entity('dat:aditid_benli#crimeLocations', {prov.model.PROV_LABEL:'Commercial Parking Ticketing', prov.model.PROV_TYPE:'ont:DataSet'})        
        getcrimeLocations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Maps crime amounts to traffic jams'})        
        doc.wasAssociatedWith(getcrimeLocations, this_script)
        doc.used(getcrimeLocations, crimeLocations, startTime)
        doc.wasAttributedTo(crimeLocations, this_script)
        doc.wasGeneratedBy(crimeLocations, getcrimeLocations, endTime)


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

transformation1.execute()
doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof