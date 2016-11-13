import dml
import prov.model
import datetime
import uuid
import json
import sys

from bson.code import Code

TRIAL_LIMIT = 5000

class transformation3(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = ['jas91_smaf91.crime', 'jas91_smaf91.sr311']
    writes = [
        'jas91_smaf91.crime_per_zip_code', 
        'jas91_smaf91.sr311_per_zip_code', 
        'jas91_smaf91.sr311_crime_per_zip_code'
    ]

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')
        
        if trial:
            print("[OUT] Running in Trial Mode")

        map_function = Code('''function() {
            id = {
                zip_code: this.geo_info.properties.zip_code,
                type: 'crime'
            }
            emit(id,1);
        }''')

        reduce_function = Code('''function(k,vs){
            return Array.sum(vs);        
        }''')
        
        if trial:
            repo.jas91_smaf91.crime.map_reduce(map_function, reduce_function, 'jas91_smaf91.crime_per_zip_code', limit=TRIAL_LIMIT)
        else:
            repo.jas91_smaf91.crime.map_reduce(map_function, reduce_function, 'jas91_smaf91.crime_per_zip_code')


        map_function = Code('''function(){
            id = {
                zip_code: this.geo_info.properties.zip_code,
                type: 'sr311'
            }
            emit(id,1);
        }''')

        if trial:
            repo.jas91_smaf91.sr311.map_reduce(map_function, reduce_function, 'jas91_smaf91.sr311_per_zip_code', limit=TRIAL_LIMIT)
        else:
            repo.jas91_smaf91.sr311.map_reduce(map_function, reduce_function, 'jas91_smaf91.sr311_per_zip_code')
        
        def union(collection1, collection2, result):
            for document in repo[collection1].find():
                repo[result].insert(document)

            for document in repo[collection2].find():
                repo[result].insert(document)

        repo.dropPermanent('jas91_smaf91.union_temp')
        repo.createPermanent('jas91_smaf91.union_temp')
        
        union('jas91_smaf91.crime_per_zip_code', 'jas91_smaf91.sr311_per_zip_code', 'jas91_smaf91.union_temp')

        map_function = Code('''function(){
            id = this._id.zip_code;
            if (this._id.type == 'sr311') {
                emit(id, {crime: 0, sr311: this.value});    
            } else {
                emit(id, {crime: this.value, sr311: 0});    
            }
        }''')
        
        reduce_function = Code('''function(k,vs){
            var total_crimes = 0
            var total_sr311 = 0
            vs.forEach(function(v, i) {
                total_crimes += v.crime;
                total_sr311 += v.sr311;
            });
            return {crime: total_crimes, sr311: total_sr311}
                    
        }''') 

        repo.jas91_smaf91.union_temp.map_reduce(map_function, reduce_function, 'jas91_smaf91.sr311_crime_per_zip_code')
        
        repo.logout()
        print('[OUT] done')

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jas91_smaf91#transformation3', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        reports = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Crimes and 311 requests per zip code.'})
        doc.wasAssociatedWith(reports, this_script)

        resource_sr311 = doc.entity('dat:jas91_smaf91#sr311', {'prov:label':'311 Service Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            reports, 
            resource_sr311, 
            startTime, 
            None,
            {
                prov.model.PROV_TYPE:'ont:Query',
                'ont:Query':'db.jas91_smaf91.sr311.find({},{geo_info: 1})'
            }
        )

        resource_crime = doc.entity('dat:jas91_smaf91#crime', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.usage(
            reports, 
            resource_crime, 
            startTime, 
            None,
            {
                prov.model.PROV_TYPE:'ont:Query',
                'ont:Query':'db.jas91_smaf91.crime.find({},{geo_info: 1})'
            }
        )

        sr311_crime_per_zip_code = doc.entity('dat:jas91_smaf91#sr311_crime_per_zip_code', {'prov:label':'Crimes and 311 requests per zip code', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(sr311_crime_per_zip_code, this_script)
        doc.wasGeneratedBy(sr311_crime_per_zip_code, reports, endTime)
        doc.wasDerivedFrom(sr311_crime_per_zip_code, resource_sr311, reports, reports, reports) 
        doc.wasDerivedFrom(sr311_crime_per_zip_code, resource_crime, reports, reports, reports) 

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

if 'trial' in sys.argv:
    transformation3.execute(True)
#else:
#    transformation3.execute()

#transformation3.execute()
#doc = transformation3.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))
