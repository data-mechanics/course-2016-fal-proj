import dml
import subprocess
import prov.model
import datetime
import uuid

class transformData(dml.Algorithm):
    contributor = 'alaw_tyroneh'
    reads = ['alaw_tyroneh.BostonProperty','alaw_tyroneh.CambridgeProperty','alaw_tyroneh.SomervilleProperty','alaw_tyroneh.BrooklineProperty']
    writes = ['alaw_tyroneh.ResidentialGeoJSONs']

    @staticmethod
    def execute():
        '''Retrieve datasets for mongoDB storage and later transformations'''
        
        startTime = datetime.datetime.now()
        
        subprocess.check_output('mongo repo -u alaw_tyroneh -p alaw_tyroneh  --authenticationDatabase "repo" data2geo.js', shell=True)
        
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
        repo.authenticate('alaw_tyroneh', 'alaw_tyroneh')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        
        repo.logout()

        return doc
    
    def run(t=False):
        '''
        Scrap datasets and write provenance files
        '''

        times = transformData.execute()

if __name__ == '__main__':
    transformData.run()
## eof