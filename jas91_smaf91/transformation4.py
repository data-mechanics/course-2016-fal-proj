import dml
import prov.model
import datetime
import uuid
import json
import sys

from bson.code import Code

TRIAL_LIMIT = 5000

def compute_hospitals_per_zip_code(repo, trial):
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
    if trial:
        repo.jas91_smaf91.hospitals.map_reduce(map_function, reduce_function, 'jas91_smaf91.hospital_per_zip_code', limit=TRIAL_LIMIT)
    else:
        repo.jas91_smaf91.hospitals.map_reduce(map_function, reduce_function, 'jas91_smaf91.hospital_per_zip_code')
        
    print('[OUT] done with hospitals')

def compute_schools_per_zip_code(repo, trial):
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
    if trial:
        repo.jas91_smaf91.schools.map_reduce(map_function, reduce_function, 'jas91_smaf91.school_per_zip_code', limit=TRIAL_LIMIT)
    else:    
        repo.jas91_smaf91.schools.map_reduce(map_function, reduce_function, 'jas91_smaf91.school_per_zip_code')

    print('[OUT] done with schools')

def compute_last_passed_inspections(repo, trial):

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
    
    if trial:
        repo.jas91_smaf91.food.map_reduce(map_function, reduce_function, 'jas91_smaf91.inspection_per_restaurant', limit=TRIAL_LIMIT)
    else:
        repo.jas91_smaf91.food.map_reduce(map_function, reduce_function, 'jas91_smaf91.inspection_per_restaurant')

def compute_passed_inspections_per_zip_code(repo, trial): 
    map_function = Code('''function() {
        id = {
            zip_code: this._id.zip_code,
            type: 'inspections'
        }

        /*
        emit(id,this.value)
        */
        if (this.value == "HE_Pass") {
            emit(id, 1);
        } else {
            emit(id, 0);
        }
    }''')

    reduce_function = Code('''function(k,vs){
        /*
        var total = 0;
        var pass = 0;
        vs.forEach(function(v, i) {
            total++;
            if (v == "HE_Pass") {
                pass++;
            }
        });
        return pass/total;
        */

        return Array.sum(vs)/vs.length;        
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

    if trial:
        repo.jas91_smaf91.inspection_per_restaurant.map_reduce( 
                map_function, 
                reduce_function, 
                'jas91_smaf91.inspection_per_zip_code', 
                finalize=finalize_function,
                limit=TRIAL_LIMIT
        )
    else:
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
    reads = [
        'jas91_smaf91.hospitals', 
        'jas91_smaf91.schools', 
        'jas91_smaf91.food', 
        'jas91_smaf91.crime_per_zip_code', 
        'jas91_smaf91.sr311_per_zip_code'
    ]
    writes = ['jas91_smaf91.attributes_per_zip_code']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        if trial:
            print("[OUT] Running in Trial Mode")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        compute_hospitals_per_zip_code(repo, trial)

        compute_schools_per_zip_code(repo, trial)
        
        compute_last_passed_inspections(repo, trial)

        compute_passed_inspections_per_zip_code(repo, trial)

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

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jas91_smaf91#transformation4', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Compute per Zipcode information'})
        
        doc.wasAssociatedWith(run, this_script)

        resource_hospitals = doc.entity('dat:jas91_smaf91#hospitals', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_food = doc.entity('dat:jas91_smaf91#food', {'prov:label':'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_schools = doc.entity('dat:jas91_smaf91#schools', {'prov:label':'Schools', prov.model.PROV_TYPE:'ont:DataSet'})

        hospitals_per_zip_code = doc.entity('dat:jas91_smaf91#hospitals_per_zip_code', {'prov:label':'Hospitals per zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        schools_per_zip_code = doc.entity('dat:jas91_smaf91#schools_per_zip_code', {'prov:label':'Schools per zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        inspections_per_zip_code = doc.entity('dat:jas91_smaf91#inspections_per_zip_code', {'prov:label':'Food Inspections per zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.usage(run, resource_hospitals, startTime, None, {})
        doc.usage(run, resource_food, startTime, None, {})
        doc.usage(run, resource_schools, startTime, None, {})

        doc.wasGeneratedBy(hospitals_per_zip_code, run, endTime)
        doc.wasGeneratedBy(schools_per_zip_code, run, endTime)
        doc.wasGeneratedBy(inspections_per_zip_code, run, endTime)
        
        doc.wasAttributedTo(hospitals_per_zip_code, this_script)
        doc.wasAttributedTo(schools_per_zip_code, this_script)
        doc.wasAttributedTo(inspections_per_zip_code, this_script)
        
        doc.wasDerivedFrom(hospitals_per_zip_code, resource_hospitals, run, run, run) 
        doc.wasDerivedFrom(schools_per_zip_code, resource_schools, run, run, run) 
        doc.wasDerivedFrom(inspections_per_zip_code, resource_food, run, run, run) 
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

if 'trial' in sys.argv:
    transformation4.execute(True)
#else:
#    transformation4.execute()
#
#doc = transformation4.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))
