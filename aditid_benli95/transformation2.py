import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class transformation2(dml.Algorithm):
    contributor = 'aditid_benli'
    reads = ['aditid_benli.jamMR', 'aditid_benli.inters']
    writes = ['aditid_benli.intersDivided','aditid_benli.jamInters']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli', 'aditid_benli')
        
        
        #Divide intersection into repsective intersecting roads
        #put into interDivided
        def divide(collection,result):
            for document in repo[collection].find():
                inter_num = (document['intersec_1'])
                inter = document['intersecti']
                rd_list = inter.split(' & ')
                #intersections have 2 to 4 roads contributing to them
                for i in range(0,len(rd_list)):
                    #change id to accomodate repeat data
                    document["_id"] = str(document["_id"]) + str(i)
                    document["road"] = rd_list[i]
                    #saving each road as new document to allow for map reduc later
                    repo[result].insert(document)


        #reset resulting directory
        repo.dropPermanent('aditid_benli.intersDivided')
        repo.createPermanent('aditid_benli.intersDivided')
        
        divide('aditid_benli.inters','aditid_benli.intersDivided')


        #add up the number of times each street is mentioned
        #this represents the number of intersections on each street
        #put into intersAdded
        map_function = Code('''function() {
            emit(this.road, {sum:1});
            }''')
        
        
        reduce_function = Code('''function(k, vs) {
            var total = 0;
            for (var i = 0; i < vs.length; i++)
            total += vs[i].sum;
            return {sum:total};
            }''')
        
        #reset resulting directory
        repo.dropPermanent('aditid_benli.intersAdded')
        repo.createPermanent('aditid_benli.intersAdded')
        
        repo.aditid_benli.intersDivided.map_reduce(map_function, reduce_function, 'aditid_benli.intersAdded');
        
        #combine number of intersections with number of jams per street
        #put into intersAdded
        def join(collection1, collection2, result):
            for document1 in repo[collection1].find():
                doc1 = str(document1['_id'])
                for document2 in repo[collection2].find():
                    doc2 = str(document2['_id'])
                    if doc1.lower() == doc2.lower():
                        document1['value']['intersections'] = document2['value']['sum']
                        repo[result].insert(document1)
    
    
        #reset resulting directory
        repo.dropPermanent('aditid_benli.jamInters')
        repo.createPermanent('aditid_benli.jamInters')
        
        join('aditid_benli.jamMR', 'aditid_benli.intersAdded', 'aditid_benli.jamInters');


        #end
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
        repo.authenticate('aditid_benli', 'aditid_benli')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aditid_benli#transformation2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        intersDivided = doc.entity('dat:aditid_benli#intersDivided', {prov.model.PROV_LABEL:'All intersections divided', prov.model.PROV_TYPE:'ont:DataSet'})
        getintersDivided = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'All interesections divided up'})        
        doc.wasAssociatedWith(getintersDivided, this_script)
        doc.used(getintersDivided, intersDivided, startTime)
        doc.wasAttributedTo(intersDivided, this_script)
        doc.wasGeneratedBy(intersDivided, getintersDivided, endTime)  


        this_script = doc.agent('alg:aditid_benli#transformation2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        jamInters = doc.entity('dat:aditid_benli#jamInters', {prov.model.PROV_LABEL:'Sums interesections for jams', prov.model.PROV_TYPE:'ont:DataSet'})
        getjamInters = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Sums the total intersections for each traffic jam'})        
        doc.wasAssociatedWith(getjamInters, this_script)
        doc.used(getjamInters, jamInters, startTime)
        doc.wasAttributedTo(jamInters, this_script)
        doc.wasGeneratedBy(jamInters, getjamInters, endTime) 


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

transformation2.execute()
doc = transformation2.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof