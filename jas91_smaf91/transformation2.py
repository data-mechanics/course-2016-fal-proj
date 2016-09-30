import dml
import prov.model
import datetime
from bson.code import Code

class transformation2(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = []
    writes = []

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

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
        
        repo.jas91_smaf91.crime.map_reduce(map_function, reduce_function, 'jas91_smaf91.crime_per_zip_code')

        map_function = Code('''function(){
            id = {
                zip_code: this.geo_info.properties.zip_code,
                type: 'sr311'
            }
            emit(id,1);
        }''')

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
        pass

transformation2.execute()
