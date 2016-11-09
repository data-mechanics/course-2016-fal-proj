import dml
import prov.model
import datetime
import uuid
import json

from bson.code import Code

def compute_hospitals_per_zip_code(repo):
    map_function = Code('''function() {
        id = {
            zip_code: this.geo_info.properties.zip_code,
            type: 'hospitals'
        }
        emit(id,1);
   }''')

    reduce_function = Code('''function(k,vs){
        return Array.sum(vs);        
    }''')

    repo.dropPermanent('jas91_smaf91.hospital_per_zip_code')
    repo.jas91_smaf91.hospitals.map_reduce(map_function, reduce_function, 'jas91_smaf91.hospital_per_zip_code')
        
    print('[OUT] done with hospitals')

def compute_schools_per_zip_code(repo):
    map_function = Code('''function() {
        id = {
            zip_code: this.geo_info.properties.zip_code,
            type: 'schools'
        }
        emit(id,1);
    }''')

    reduce_function = Code('''function(k,vs){
        return Array.sum(vs);        
    }''')

    repo.dropPermanent('jas91_smaf91.school_per_zip_code')
    repo.jas91_smaf91.schools.map_reduce(map_function, reduce_function, 'jas91_smaf91.school_per_zip_code')

    print('[OUT] done with schools')

def compute_last_passed_inspections(repo):

    map_function = Code('''function() {

        id = {
            zip_code: this.geo_info.properties.zip_code,
            property_id : this.property_id,
            businessname: this.businessname
        }
        data = {
            result: this.result,
            resultdttm: new Date(this.resultdttm)
        }

        if (this.property_id) {
            emit(id, data);
        }
    }''')

    reduce_function = Code('''function(k,vs){
        max_date = new Date(-8640000000000000)
        max_index = -1;
        vs.forEach(function(v, i) {
            if (v) {
                if (v.resultdttm > max_date) {
                    max_date = v;
                    max_index = i;
                }
            }
        });

        if (max_index >= 0) {
            return vs[max_index].result
        }
    }''')

    repo.dropPermanent('jas91_smaf91.inspection_per_restaurant')
    repo.jas91_smaf91.food.map_reduce(map_function, reduce_function, 'jas91_smaf91.inspection_per_restaurant')

def compute_passed_inspections_per_zip_code(repo): 
    map_function = Code('''function() {
        id = {
            zip_code: this._id.zip_code,
            type: 'inspections'
        }
        emit(id, this.value);
    }''')

    reduce_function = Code('''function(k,vs){
        var total = 0;
        var pass = 0;
        vs.forEach(function(v, i) {
            total++;
            if (v == "HE_Pass") {
                pass++;
            }
        });
        return pass/total;
    }''')

    finalize_function = Code('''function(k,v) {
        if (v == "HE_Fail") {
            return 0;
        } else if (v == "HE_Pass") {
            return 1;    
        }
        return v;
    }''')

    repo.dropPermanent('jas91_smaf91.inspection_per_zip_code')
    repo.jas91_smaf91.inspection_per_restaurant.map_reduce(
        map_function, 
        reduce_function, 
        'jas91_smaf91.inspection_per_zip_code', 
        finalize=finalize_function
    )
    repo.dropPermanent('jas91_smaf91.inspection_per_restaurant')

    print('[OUT] done with inspections')

def merge_datasets(repo):
    def add(collection1, result):
        for document in repo[collection1].find():
            repo[result].insert(document)

    repo.dropPermanent('jas91_smaf91.union_temp')
    repo.createPermanent('jas91_smaf91.union_temp')
    add('jas91_smaf91.inspection_per_zip_code', 'jas91_smaf91.union_temp')
    add('jas91_smaf91.school_per_zip_code', 'jas91_smaf91.union_temp')
    add('jas91_smaf91.hospital_per_zip_code', 'jas91_smaf91.union_temp')
    add('jas91_smaf91.crime_per_zip_code', 'jas91_smaf91.union_temp')
    add('jas91_smaf91.sr311_per_zip_code', 'jas91_smaf91.union_temp')

    print('[OUT] done with the union')

def compute_attributes_per_zip_code(repo):
    map_function = Code('''function(){
        id = this._id.zip_code;
        if (this._id.type == 'inspections') {
            emit(id, {hospitals: 0, schools: 0, inspections: this.value, crimes: 0, sr311:0});    
        } else if (this._id.type == 'hospitals') {
            emit(id, {hospitals: this.value, schools: 0, inspections: 0, crimes:0, sr311:0});    
        } else if (this._id.type == 'schools'){
            emit(id, {hospitals: 0, schools: this.value, inspections: 0, crimes:0, sr311:0});    
        } else if (this._id.type == 'crimes'){
            emit(id, {hospitals: 0, schools: 0, inspections: 0, crimes: this.value, sr311:0});    
        } else if (this._id.type == 'sr311'){
            emit(id, {hospitals: 0, schools: 0, inspections: 0, crimes:0, sr311: this.value});    
        }
    }''')
   
    reduce_function = Code('''function(k,vs){
        var total_hospitals = 0
        var total_schools = 0
        var total_inspections = 0
        var total_crimes = 0
        var total_sr311 = 0
        vs.forEach(function(v, i) {
            total_hospitals += v.hospitals;
            total_schools += v.schools;
            total_inspections += v.inspections;
            total_crimes += v.crimes;
            total_sr311 += v.sr311;
        });
        return {hospitals: total_hospitals, schools: total_schools, inspections: total_inspections, crimes: total_crimes, sr311: total_sr311}
    }''') 

    repo.createPermanent('jas91_smaf91.attributes_per_zip_code')
    repo.jas91_smaf91.union_temp.map_reduce(map_function, reduce_function, 'jas91_smaf91.attributes_per_zip_code')

    print('[OUT] done with combining')

def drop_temporary_datasets(repo):
    repo.dropPermanent('jas91_smaf91.union_temp')
    repo.dropPermanent('jas91_smaf91.inspection_per_restaurant')
    repo.dropPermanent('jas91_smaf91.inspection_per_zip_code')
    repo.dropPermanent('jas91_smaf91.school_per_zip_code')
    repo.dropPermanent('jas91_smaf91.hospital_per_zip_code')

class transformation4(dml.Algorithm):
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

        compute_hospitals_per_zip_code(repo)

        compute_schools_per_zip_code(repo)
        
        compute_last_passed_inspections(repo)

        compute_passed_inspections_per_zip_code(repo)

        merge_datasets(repo)

        compute_attributes_per_zip_code(repo)

        drop_temporary_datasets(repo)

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
