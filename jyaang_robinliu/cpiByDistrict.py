import json
import urllib.request #request the JSON url
import dml
import datetime
import pymongo
from pymongo import MongoClient
import ast
import uuid
import prov.model
from bson.code import Code


class cpiByDistrict(dml.Algorithm):
    contributor = 'jyaang_robinliu106'
    reads = []
    writes = ['jyaang_robinliu106.totalCPI_district']


    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')
        url = "file:///Users/jinnyyang/Desktop/course-2016-fal-proj-master/jyaang_robinliu106/mass_districts.json"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        districts = json.loads(response)
        jsonTestScores = json.dumps(districts, sort_keys=True, indent=2)


        #with open('mass_districts.json') as json_data:
            #districts = json.load(json_data)

        #Change key name from District Name to Name
        for entry in districts:
            for key in entry:
                if key == "District Name":
                    entry["Name"] = entry.pop(key).upper()
                if key == "Zip Code":
                    entry["Zip"] = entry.pop(key)



        repo.dropPermanent("district_zipcode")
        repo.createPermanent("district_zipcode")

        repo.dropPermanent("zip_city_score")
        repo.createPermanent("zip_city_score")
        repo['jyaang_robinliu106.district_zipcode'].insert_many(districts)

        scores = repo["jyaang_robinliu106.testScores"]

        map = Code("function() {emit(this.Name, {Name: this.Name, CPI:this.CPI});}")
        reduce = Code("function(k, vs) {var total = 0; var loops = 0; for(var i = 0; i < vs.length; i++)total += vs[i].CPI, loops += 1; if(loops>1)total = total/loops; return {Name: k, CPI:total};}")
        repo.dropPermanent("totalCPI_district")
        repo.createPermanent("totalCPI_district")
        results = scores.map_reduce(map,reduce, "jyaang_robinliu106.totalCPI_district")

        #cursor = repo["jyaang_robinliu106.totalCPI_district"].find()


        #repo["jyaang_robinliu106.zip_city_score"].insert_many(cursor)

        district_map = Code("function() {emit(this._id, {Zip:this.Zip,Name:this.Name});};")
        scores_map = Code("function() {emit(this.value.Name, {Score: this.CPI});};")
        r = Code("function(key, values){var result={Zip:0, Score:0, Name:values.Name}; values.forEach(function(values){if(value.Zip!==null && value.Zip == 0){result.Zip = value.Zip;} if(value.Score!=null && value.Score == 0){result.Score = value.Score;}}); return result;}")
        #r = Code("function(key, values){var result={Zip:0, Score:0}; values.forEach(function(values){if(value.Zip!==null && value.Zip == 0){result.Zip = value.Zip;} if(value.Score!=null && value.Score == 0){result.Score = value.Score;}}); return result;}")

        res = repo["jyaang_robinliu106.district_zipcode"].map_reduce(district_map, r, "jyaang_robinliu106.district_zipcode")




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

        this_script = doc.agent('alg:jyaang_robinliu106#getTestScores', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'testScore Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_testScore = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_testScore, this_script)
        doc.usage(get_testScore, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=testScore&$select=type,latitude,longitude,OPEN_DT'
                }
            )

        testScore = doc.entity('dat:jyaang_robinliu106#testScore', {prov.model.PROV_LABEL:'testScore Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(testScore, this_script)
        doc.wasGeneratedBy(testScore, get_testScore, endTime)
        doc.wasDerivedFrom(testScore, resource, get_testScore, get_testScore, get_testScore)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

cpiByDistrict.execute()
doc = cpiByDistrict.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
