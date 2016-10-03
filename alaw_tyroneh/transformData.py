import dml
import prov.model
import datetime
import uuid

class transformData(dml.Algorithm):
    contributor = 'alaw_tyroneh'
    reads = []
    writes = ['alaw_tyroneh.MBTABusStops']

    @staticmethod
    def execute(trial = False):
        '''Retrieve datasets for mongoDB storage and later transformations'''
        
        startTime = datetime.datetime.now()
            
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('alaw_tyroneh', 'alaw_tyroneh')
        
        
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
        repo.authenticate('alaw_tyroneh', 'alaw_tyroneh')
        
        
        repo.logout()

        return doc
    
    def run(t=False):
        '''
        Scrap datasets and write provenance files
        '''

        times = transformData.execute(trial=True)

if __name__ == '__main__':
    transformData.run()
## eof