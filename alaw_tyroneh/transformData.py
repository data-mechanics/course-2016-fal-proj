import dml
import subprocess
import prov.model
import datetime
import uuid

class transformData(dml.Algorithm):
    contributor = 'alaw_tyroneh'
    reads = ['alaw_tyroneh.BostonProperty','alaw_tyroneh.CambridgeProperty','alaw_tyroneh.SomervilleProperty','alaw_tyroneh.BrooklineProperty','alaw_tyroneh.HubwayStations','alaw_tyroneh.TCStops']
    writes = ['alaw_tyroneh.ResidentialGeoJSONs','alaw_tyroneh.StationGeoJSONs']

    @staticmethod
    def execute():
        '''Retrieve datasets for mongoDB storage and later transformations'''
        
        startTime = datetime.datetime.now()
        
        subprocess.check_output('mongo repo -u alaw_tyroneh -p alaw_tyroneh  --authenticationDatabase "repo" data2Geo.js', shell=True)
        
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
        
        this_script = doc.agent('alg:alaw_tyroneh#transformData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        residential = doc.entity('dat:alaw_tyroneh#ResidentialGeoJSONs', {prov.model.PROV_LABEL:'Residential GeoJSONs', prov.model.PROV_TYPE:'ont:DataSet'})        
        get_residential = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'MapReduce to produce Residential GeoJSONs'})        
        doc.wasAssociatedWith(get_residential, this_script)
        doc.used(get_residential, residential, startTime)
        doc.wasAttributedTo(residential, this_script)
        doc.wasGeneratedBy(residential, get_residential, endTime)        
        
        stations = doc.entity('dat:alaw_tyroneh#StationGeoJSONs', {prov.model.PROV_LABEL:'Station GeoJSONs', prov.model.PROV_TYPE:'ont:DataSet'})         
        get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'MapReduce to produce Station GeoJSONs'})          
        doc.wasAssociatedWith(get_stations, this_script)
        doc.used(get_stations, stations, startTime)
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, get_stations, endTime)          
        
        repo.logout()

        return doc
    
    def run(t=False):
        '''
        Scrap datasets and write provenance files
        '''

        times = transformData.execute()
        transformData.provenance(startTime = times['start'], endTime = times['end'])

if __name__ == '__main__':
    transformData.run()
## eof
