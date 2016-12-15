
# coding: utf-8

# In[8]:

import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code


class megaMapReduce(dml.Algorithm):

    contributor = 'aditid_benli95_teayoon_tyao'
    reads = ['aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius', 'aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug']
    writes = ['aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment', 'aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment', 'aditid_benli95_teayoon_tyao.averageAll', 'aditid_benli95_teayoon_tyao.averageDrug']


    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        print("hello from megaMapReduce")
        
        """
        CHANGE RADIUS VALUE HERE!!
        """
        radius = 5
        
        

        ''' This commented out portion will be important for data visualisation later on '''

        #'''The following map reduce code takes the numberOfEstablishmentsinRadius and the numberOfEstablishmentsinRadiusDrug repos and returns a distribution of the number of crimes that have x children establishments within a certain proximity '''
        #
        ##find the number of crimes with the same number of establishments
        #map_function = Code('''function() {
        #    emit(this.total, {count:1, fake:0});
        #    }''')
        #
        #
        #reduce_function = Code('''function(k, vs) {
        #    var total = 0;
        #    for (var i = 0; i < vs.length; i++)
        #    total += vs[i].count;
        #    return {count:total, fake: 0};
        #    }''')
        #
        ##reset resulting directory
        #repo.dropPermanent('aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment')
        #repo.createPermanent('aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment')
        #
        #repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius.map_reduce(map_function, reduce_function, 'aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment');
        #
        #
        ##find the number of drug crimes with the same number of establishments
        #
        #
        ##reset resulting directory
        #repo.dropPermanent('aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment')
        #repo.createPermanent('aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment')
        #
        #repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug.map_reduce(map_function, reduce_function, 'aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment');


        '''The following map reduce code takes the numberOfEstablishmentsinRadius and the 
        numberOfEstablishmentsinRadiusDrug repos and returns a distribution of the number of crimes 
        that have x children establishments within a certain proximity as well as the product of 
        the crimes by establishments and a temporary variable (temp) that will allow for the entire 
        repo to be collapsed into a single key during the next map reduce '''


        map_function = Code('''function() {
            emit(this.total, {crimes:1, total:this.total, product:this.total, temp:5});
            }''')


        reduce_function = Code('''function(k, vs) {
            var total_crimes = 0;
            var tot = vs[0].total;
            
            for (var i = 0; i < vs.length; i++)
            total_crimes += vs[i].crimes;
            
            var prod = tot * total_crimes
            return {crimes:total_crimes, total:vs[0].total, product: prod, temp:5};
            }''')

        base_source_all = 'repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius'
        base_source_drugs = 'repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug'

        base_result_all = 'aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment'
        base_result_drugs = 'aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment'

        for num in range(0,10):
            add_on = str((radius*10) + num)      #creates string of integer values (50, 51, ...)

            repo_source_all = base_source_all + add_on
            repo_result_all = base_result_all + add_on

            repo.dropPermanent(repo_result_all)
            repo.createPermanent(repo_result_all)

            string_for_execution_all = repo_source_all + ".map_reduce(map_function, reduce_function, '" + repo_result_all + "')"
            exec(string_for_execution_all)

            repo_source_drug = base_source_drugs + add_on
            repo_result_drug = base_result_drugs + add_on

            repo.dropPermanent(repo_result_drug)
            repo.createPermanent(repo_result_drug)

            string_for_execution_drug = repo_source_drug + ".map_reduce(map_function, reduce_function, '" + repo_result_drug + "')"
            #exec(string_for_execution_drug)
                
                
                
        repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadius.map_reduce(map_function, reduce_function, 'aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment');
        repo.aditid_benli95_teayoon_tyao.numberOfEstablishmentsinRadiusDrug.map_reduce(map_function, reduce_function, 'aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment');


        ''' This takes the previous map reduced repositories and returns the total sum of establishments around crimes and the number of crimes. Using these values the average number of establishments around each crime can be calculated.'''


        #find the number of crimes with the same number of establishments
        map_function = Code('''function() {
            for(var i in this.value) {
            emit(this.value[i].temp, {crimes:this.value.crimes, product:this.value.product});
            break;
            }
            }''')


        reduce_function = Code('''function(k, vs) {
            var total_crime = 0;
            var total_prod = 0;
            for (var i = 0; i < vs.length; i++)
            {
            total_crime += vs[i].crimes;
            total_prod += vs[i].product;
            }
            return {crimes:total_crime, product:total_prod};
            }''')

        base_source_all = 'repo.aditid_benli95_teayoon_tyao.crimesPerNumberOfEstablishment'
        base_source_drugs = 'repo.aditid_benli95_teayoon_tyao.drugCrimesPerNumberOfEstablishment'

        base_result_all = 'aditid_benli95_teayoon_tyao.averageAll'
        base_result_drugs = 'aditid_benli95_teayoon_tyao.averageDrug'

        for num in range(0,10):
            add_on = str((radius*10) + num)      #creates string of integer values (50, 51, ...)

            repo_source_all = base_source_all + add_on
            repo_result_all = base_result_all + add_on

            repo.dropPermanent(repo_result_all)
            repo.createPermanent(repo_result_all)

            string_for_execution_all = repo_source_all + ".map_reduce(map_function, reduce_function, '" + repo_result_all + "')"
            exec(string_for_execution_all)

            repo_source_drug = base_source_drugs + add_on
            repo_result_drug = base_result_drugs + add_on

            repo.dropPermanent(repo_result_drug)
            repo.createPermanent(repo_result_drug)

            string_for_execution_drug = repo_source_drug + ".map_reduce(map_function, reduce_function, '" + repo_result_drug + "')"
            #exec(string_for_execution_drug)
        
        
        endTime = datetime.datetime.now()
        return {"Start ":startTime, "End ":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli95_teayoon_tyao', 'aditid_benli95_teayoon_tyao')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cob', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:aditid_benli95_teayoon_tyao#megaMapReduce', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        megaMR = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime, {'prov:label':'Mega Map Reduce', prov.model.PROV_TYPE:'ont:Computation'})
        doc.wasAssociatedWith(megaMR, this_script)

        numberOfEstablishmentsinRadius = doc.entity('dat:aditid_benli95_teayoon_tyao#numberOfEstablishmentsinRadius', {'prov:label':'Number Of Establishments near All Crimes', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(megaMR, numberOfEstablishmentsinRadius, startTime)

        numberOfEstablishmentsinRadiusDrug = doc.entity('dat:aditid_benli95_teayoon_tyao#numberOfEstablishmentsinRadiusDrug', {'prov:label':'Number Of Establishments near Drug Crimes', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.usage(megaMR, numberOfEstablishmentsinRadiusDrug, startTime)

        crimesPerNumberOfEstablishment = doc.entity('dat:aditid_benli95_teayoon_tyao#crimesPerNumberOfEstablishment', {'prov:label':'Number Of All Crimes per Establishments', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.wasAttributedTo(crimesPerNumberOfEstablishment, this_script)
        doc.wasGeneratedBy(crimesPerNumberOfEstablishment, megaMR, endTime)
        doc.wasDerivedFrom(crimesPerNumberOfEstablishment, numberOfEstablishmentsinRadius, megaMR, megaMR, megaMR)
        
        drugCrimesPerNumberOfEstablishment = doc.entity('dat:aditid_benli95_teayoon_tyao#drugCrimesPerNumberOfEstablishment', {'prov:label':'Number Of Drug Crimes per Establishments', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.wasAttributedTo(drugCrimesPerNumberOfEstablishment, this_script)
        doc.wasGeneratedBy(drugCrimesPerNumberOfEstablishment, megaMR, endTime)
        doc.wasDerivedFrom(drugCrimesPerNumberOfEstablishment, numberOfEstablishmentsinRadiusDrug, megaMR, megaMR, megaMR)

        averageAll = doc.entity('dat:aditid_benli95_teayoon_tyao#averageAll', {'prov:label':'Average of Establishments near All Crimes', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.wasAttributedTo(averageAll, this_script)
        doc.wasGeneratedBy(averageAll, megaMR, endTime)
        doc.wasDerivedFrom(averageAll, numberOfEstablishmentsinRadius, megaMR, megaMR, megaMR)

        averageDrug = doc.entity('dat:aditid_benli95_teayoon_tyao#averageDrug', {'prov:label':'Average of Establishments near Drug Crimes', prov.model.PROV_TYPE:'ont:Dataset'})
        doc.wasAttributedTo(averageDrug, this_script)
        doc.wasGeneratedBy(averageDrug, megaMR, endTime)
        doc.wasDerivedFrom(averageDrug, numberOfEstablishmentsinRadiusDrug, megaMR, megaMR, megaMR)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

megaMapReduce.execute()
doc = megaMapReduce.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))




# In[ ]:



