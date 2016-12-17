import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
    
class mapReduceLights(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = ['jzhou94_katerin.lights_coordinates']
    writes = ['jzhou94_katerin.mapReduceLights']
        
    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        map_function_lights = Code('''function() {
            emit(this.zip, {lights:1});
            }''')

        reduce_function_lights = Code('''function(k, vs) {
            var total = 0;
            for (var i = 0; i < vs.length; i++)
                total += vs[i].lights;
            return {lights:total};
            }''')

        

        repo.dropPermanent('jzhou94_katerin.mapReduceLights')
        repo.createPermanent('jzhou94_katerin.mapReduceLights')

        if trial == True:
            repo['jzhou94_katerin.mapReduceLights'].insert(repo.jzhou94_katerin.lights_coordinates.find().limit(100))
            repo.jzhou94_katerin.mapReduceLights.map_reduce(map_function_lights, reduce_function_lights, 'jzhou94_katerin.mapReduceLights');
        else:
            repo.jzhou94_katerin.lights_coordinates.map_reduce(map_function_lights, reduce_function_lights, 'jzhou94_katerin.mapReduceLights');

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

        this_script = doc.agent('alg:mapReduceLights', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        lights_coordinates = doc.entity('dat:lights_coordinates', {prov.model.PROV_LABEL:'Employee Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
        get_numlights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_numlights, this_script)
        doc.usage(get_numlights, lights_coordinates, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )

        numlights = doc.entity('dat:mapReduceLights', {'prov:label':'Average Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(numlights, this_script)
        doc.wasGeneratedBy(numlights, get_numlights, endTime)
        doc.wasDerivedFrom(numlights, lights_coordinates, get_numlights, get_numlights, get_numlights)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

mapReduceLights.execute()
print("mapReduceLights Algorithm Done")
doc = mapReduceLights.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("mapReduceLights Provenance Done")

## eof
