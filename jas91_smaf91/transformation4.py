import dml
import prov.model
import datetime
import uuid
import json

from bson.code import Code

class transformation4(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = ['jas91_smaf91.crime', 'jas91_smaf91.sr311']
    writes = ['jas91_smaf91.sr311_crime_per_zip_code']

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
                type: 'hospital'
            }
            emit(id,1);
        }''')

        reduce_function = Code('''function(k,vs){
            return Array.sum(vs);        
        }''')

        repo.dropPermanent('jas91_smaf91.hospital_per_zip_code')
        repo.jas91_smaf91.hospitals.map_reduce(map_function, reduce_function, 'jas91_smaf91.hospital_per_zip_code')
        
        print('[OUT] done with hospitals')

        map_function = Code('''function() {
            id = {
                zip_code: this.geo_info.properties.zip_code,
                type: 'school'
            }
            emit(id,1);
        }''')

        repo.dropPermanent('jas91_smaf91.school_per_zip_code')
        repo.jas91_smaf91.schools.map_reduce(map_function, reduce_function, 'jas91_smaf91.school_per_zip_code')
        
        print('[OUT] done with schools')

        map_function = Code('''function() {
            id = {
                zip_code: this.geo_info.properties.zip_code,
                property_id : this.property_id
            }
            data = {
                result: this.result,
                resultdttm: new Date(this.resultdttm)
            }
            emit(id, data);
        }''')

        reduce_function = Code('''function(k,vs){
            max_date = vs[0].resultdttm;
            max_index = 0;
            vs.forEach(function(v, i) {
                if (v.resultdttm > max_date) {
                    max_date = v;
                    max_index = i;
                }
            });

            return vs[max_index].result
        }''')

        repo.dropPermanent('jas91_smaf91.inspection_per_restaurant')
        repo.jas91_smaf91.food.map_reduce(map_function, reduce_function, 'jas91_smaf91.inspection_per_restaurant')

        map_function = Code('''function() {
            id = {
                zip_code: this._id.zip_code,
                type: 'food'
            }
            emit(id, this.value);
        }''')

        reduce_function = Code('''function(k,vs){
            return vs.length
        }''')

        repo.dropPermanent('jas91_smaf91.inspection_per_zip_code')
        repo.jas91_smaf91.inspection_per_restaurant.map_reduce(map_function, reduce_function, 'jas91_smaf91.inspection_per_zip_code')
        # repo.dropPermanent('jas91_smaf91.inspection_per_restaurant')

        print('[OUT] done with inspections')

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''
        pass

transformation4.execute()
'''
doc = transformation4.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
