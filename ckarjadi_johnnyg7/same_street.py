import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from street_scripts import *

class same_street(dml.Algorithm):
    contributor = 'ckarjadi_johnnyg7'
    reads = []
    writes = ['ckarjadi_johnnyg7.same_street']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        AWZ = get_Col("Active_Work_Zones",repo)
        Waze_Jams= get_Col("Waze_Jams",repo)
    
        waze = get_waze(Waze_Jams,'street','avg_jam_duration')
        master_streets = waze[1]
        master = waze[0]
        
        master = get_AWZ(AWZ,'street',master_streets,master)        
        #test = [{'1':'h'}]
        print(master)

        repo.dropPermanent("same_street")
        repo.createPermanent("same_street")
        repo['ckarjadi_johnnyg7.same_street'].insert_many(master)
        
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
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ckarjadi_johnnyg7', 'ckarjadi_johnnyg7')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        #<ckarjadi_johnnyg7>#<somefile_name>
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        this_script = doc.agent('alg:same_street', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('dat:waze_jams;active work zones', \
            {'prov:label':'Waze Jams and Active work Zones reduced by street', \
             prov.model.PROV_TYPE:'ont:DataResource', prov.model.PROV_TYPE:'ont:Computation'})
        get_same_street = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_same_street, this_script)
        doc.usage(get_same_street, resource1, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'???'
                }
            )
        same_street = doc.entity('dat:ckarjadi_johnnyg7#same_street', {prov.model.PROV_LABEL:'Active Work Zones and Waze Jams reduced by street', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(same_street, this_script)
        doc.wasGeneratedBy(same_street, get_same_street, endTime)
        doc.wasDerivedFrom(same_street, resource1, get_same_street, get_same_street, get_same_street)
        repo.record(doc.serialize())
        repo.logout()
        return doc
same_street.execute()
doc = same_street.provenance()
##print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
