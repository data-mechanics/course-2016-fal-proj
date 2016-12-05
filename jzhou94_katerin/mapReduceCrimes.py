import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code


class mapReduceCrimes(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = ['jzhou94_katerin.crime_incident']
    writes = ['jzhou94_katerin.crime']
        
    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')
    
        map_function_crime = Code('''
            function() {
            district = this.reptdistrict
            if(district == 'A1' || district == 'A15')
                emit('02120', {crime:1});
            else if(district == 'A7')
                emit('02128', {crime:1});
            else if(district == 'B2')
                emit('02119', {crime:1});
            else if(district == 'B3')
                emit('02124', {crime:1});
            else if(district == 'C6')
                emit('02127', {crime:1});
            else if(district == 'C11')
                emit('02122', {crime:1});
            else if(district == 'D4')
                emit('02116', {crime:1});
            else if(district == 'D14')
                emit('02135', {crime:1});
            else if(district == 'E5')
                emit('02132', {crime:1});
            else if(district == 'E13')
                emit('02130', {crime:1});
            else if(district == 'E18')
                emit('02136', {crime:1});
            }''')
        
        reduce_function_crime = Code('''function(k, vs) {
            var total = 0;
            for (var i = 0; i < vs.length; i++)
               total += vs[i].crime;
            return {crime:total};
            }''')
    
        repo.dropPermanent('jzhou94_katerin.crime')
        repo.createPermanent('jzhou94_katerin.crime')


        if trial == True:
            repo['jzhou94_katerin.crime'].insert(repo.jzhou94_katerin.crime_incident.find().limit(20))
            repo.jzhou94_katerin.crime.map_reduce(map_function_crime, reduce_function_crime, 'jzhou94_katerin.crime');
        else:
            repo.jzhou94_katerin.crime_incident.map_reduce(map_function_crime, reduce_function_crime, 'jzhou94_katerin.crime');

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

        this_script = doc.agent('alg:mapReduceCrimes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        crime_incident = doc.entity('dat:crime_incident', {prov.model.PROV_LABEL:'Crime Incident', prov.model.PROV_TYPE:'ont:DataSet'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, crime_incident, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )

        crime = doc.entity('dat:crime', {prov.model.PROV_LABEL:'Crimes per Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, crime_incident, get_crime, get_crime, get_crime)


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

mapReduceCrimes.execute()
print("mapReduceCrimes Algorithm Done")
doc = mapReduceCrimes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("mapReduceCrimes Provenance Done")

## eof
