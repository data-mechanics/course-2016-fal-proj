import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.json_util import dumps
from helpers import *



class PublicandEarning(dml.Algorithm):
    contributor = 'aydenbu_huangyh'
    reads = ['aydenbu_huangyh.public_earning','aydenbu_huangyh.zip_crime_count']
    writes = ['aydenbu_huangyh.public_earning_crime']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection
        repo = openDb(getAuth("db_username"), getAuth("db_password"))
        publics = repo['aydenbu_huangyh.public_earning']
        crimes = repo['aydenbu_huangyh.zip_crime_count']

        zipInBoston = ['2151','2128','2129','2113','2114','2203','2109','2108','2110','2111','2116','2210','2199','2127','2118','2115','2215','2125','2120','2119',
                       '2163','2134','2135','2467','2130','2121','2122','2467','2131','2126','2124','2132','2131','2126','2136']
        # reference http://www.cityofboston.gov/images_documents/ZipCodes_tcm3-47884.pdf

        publics_array = []
        for document in publics.find():
           # if int(str(document['_id']),10) != 0:
                publics_array.append({"zip":document['_id'], 'value':{'avg': document['value']['avg'],
                                                                        'numofHospital': document['value']['numofHospital'],
                                                                        'numofSchool': document['value']['numofSchool'],
                                                                        'numofGarden':document['value']['numofGarden'],
                                                                         'numofStore': document['value']['numofStore'],
                                                                         'numofCrime': 0 }})

        crimes_array = []
        for document in crimes.find():
            crimes_array.append({"zip":document['_id'], "value":{'avg': 0,
                                                                  'numofSchool': 0,
                                                                  'numofHospital': 0,
                                                                  'numofGarden': 0,
                                                                   'numofStore': 0,
                                                                   'numofCrime': document['value']['numofCrime']
                                                                  }})

        repo.dropPermanent("test2")
        repo.createPermanent("test2")
        repo['aydenbu_huangyh.test2'].insert_many(publics_array)
        repo['aydenbu_huangyh.test2'].insert_many(crimes_array)

        test2 = repo['aydenbu_huangyh.test2']

        # MapReduce function






        map = Code("""
                    function(){
                           emit(this.zip,this.value);
                            }
                    """)

        reducer = Code("""
                function(key,values){

                        var result = {
                        "avg": 0,
                        "numofSchool" : 0,
                        "numofHospital" : 0,
                        "numofGarden": 0,
                        "numofStore": 0,
                        "numofCrime": 0
                        };


                        values.forEach(function(value) {
                        if(value.avg !== 0) {result.avg = value.avg;}
                        if(value.numofHospital !== 0) {result.numofHospital = value.numofHospital;}
                        if(value.numofSchool !== 0) {result.numofSchool = value.numofSchool;}
                        if(value.numofGarden !== 0) {result.numofGarden = value.numofGarden;}
                        if(value.numofStore !== 0) {result.numofStore = value.numofStore;}
                        if(value.numofStore !== 0) {result.numofCrime = value.numofCrime;}



                         });

                        return result;
                        }





                        """)




        repo.dropPermanent("public_earning_crime")

        res = test2.map_reduce(map, reducer, 'aydenbu_huangyh.public_earning_crime')




        allData = repo['aydenbu_huangyh.public_earning_crime']
        filterResult = []
        for document in allData.find():
            if document['_id'] in zipInBoston:
                filterResult.append(document)
            else:
                continue

        repo.dropPermanent("public_earning_crime_boston")
        repo.createPermanent("public_earning_crime_boston")
        repo['aydenbu_huangyh.public_earning_crime_boston'].insert_many(filterResult)


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection
        repo = openDb(getAuth("db_username"), getAuth("db_password"))

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aydenbu_huangyh#public_earning',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_public = doc.entity('dat:zip_public',
                              {'prov:label': 'zip_public', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})
        resource_earning = doc.entity('dat:zip_avg_earnings',
                                     {'prov:label': 'zip_avg_earnings', prov.model.PROV_TYPE: 'ont:DataResource',
                                      'ont:Extension': 'json'})


        get_public_earning = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                               {prov.model.PROV_LABEL: "Count the avg earnings and number of public buildings"})
        doc.wasAssociatedWith(get_public_earning, this_script)

        doc.usage(get_public_earning, resource_public, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(get_public_earning, resource_earning, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        public_earning = doc.entity('dat:aydenbu_huangyh#public_earning',
                                         {prov.model.PROV_LABEL: 'Public Building and avg earnings count',
                                          prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(public_earning, this_script)
        doc.wasGeneratedBy(public_earning,get_public_earning, endTime)
        doc.wasDerivedFrom(public_earning, resource_earning, get_public_earning, get_public_earning,
                           get_public_earning)
        doc.wasDerivedFrom(public_earning, resource_public, get_public_earning, get_public_earning,
                           get_public_earning)

        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


PublicandEarning.execute()
doc = PublicandEarning.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## add provenance
## eof
