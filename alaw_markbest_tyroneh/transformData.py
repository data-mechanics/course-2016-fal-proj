import dml
import subprocess
import prov.model
import datetime
import uuid

class transformData(dml.Algorithm):
    contributor = 'alaw_markbest_tyroneh'
    reads = ['alaw_markbest_tyroneh.BostonProperty','alaw_markbest_tyroneh.CambridgeProperty','alaw_markbest_tyroneh.SomervilleProperty','alaw_markbest_tyroneh.BrooklineProperty','alaw_markbest_tyroneh.HubwayStations','alaw_markbest_tyroneh.TCStops']
    writes = ['alaw_markbest_tyroneh.PropertyGeoJSONs','alaw_markbest_tyroneh.StationGeoJSONs']

    @staticmethod
    def execute():
        '''Retrieve datasets for mongoDB storage and later transformations'''
        
        startTime = datetime.datetime.now()
        
        subprocess.check_output('mongo repo -u alaw_markbest_tyroneh -p alaw_markbest_tyroneh  --authenticationDatabase "repo" data2Geo.js', shell=True)

        subprocess.check_output('mongo repo -u alaw_markbest_tyroneh -p alaw_markbest_tyroneh  --authenticationDatabase "repo" getAvgVels.js', shell=True)

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
        repo.authenticate('alaw_markbest_tyroneh', 'alaw_markbest_tyroneh')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        this_script = doc.agent('alg:alaw_markbest_tyroneh#transformData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        Properties = doc.entity('dat:alaw_markbest_tyroneh#PropertyGeoJSONs', {prov.model.PROV_LABEL:'Properties GeoJSONs', prov.model.PROV_TYPE:'ont:DataSet'})        
        get_Properties = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'MapReduce to produce Properties GeoJSONs'})        
        doc.wasAssociatedWith(get_Properties, this_script)
        doc.used(get_Properties, Properties, startTime)
        doc.wasAttributedTo(Properties, this_script)
        doc.wasGeneratedBy(Properties, get_Properties, endTime)        
        
        stations = doc.entity('dat:alaw_markbest_tyroneh#StationGeoJSONs', {prov.model.PROV_LABEL:'Station GeoJSONs', prov.model.PROV_TYPE:'ont:DataSet'})         
        get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'MapReduce to produce Station GeoJSONs'})          
        doc.wasAssociatedWith(get_stations, this_script)
        doc.used(get_stations, stations, startTime)
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, get_stations, endTime)          
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc
    
    def run():
        '''
        Scrap datasets and write provenance files
        '''

        times = transformData.execute()
        transformData.provenance(startTime = times['start'], endTime = times['end'])

if __name__ == '__main__':
    transformData.run()
## eof
