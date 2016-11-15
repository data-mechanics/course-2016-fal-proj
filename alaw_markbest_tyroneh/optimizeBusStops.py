import dml
import prov.model
import datetime
import uuid

class optimizeBusStops(dml.Algorithm):
    contributor = 'alaw_markbest_tyroneh'
    reads = ['alaw_markbest_tyroneh.BusRoutes','alaw_markbest_tyroneh.StationsGeoJSONs']
    writes = ['alaw_markbest_tyroneh.NewStops']

    @staticmethod
    def execute(trial = False, visual = False):	
        startTime = datetime.datetime.now()
        
        subprocess.check_output('mongo repo -u alaw_markbest_tyroneh -p alaw_markbest_tyroneh --authenticationDatabase "repo" optimizeBusStops.js', shell=True)

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

        this_script = doc.agent('alg:alaw_markbest_tyroneh#optimizeBusStops', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        BusRoutes = doc.entity('dat:alaw_markbest_tyroneh#BusRoutes', {prov.model.PROV_LABEL:'Bus Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        Stations = doc.entity('dat:alaw_markbest_tyroneh#StationsGeoJSONs', {prov.model.PROV_LABEL:'T, Commuter Rail, and Hubway Stations', prov.model.PROV_TYPE:'ont:DataSet'})

        NewStops = doc.entity('dat:alaw_markbest_tyroneh#NewStops', {prov.model.PROV_LABEL:'New Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        get_NewStops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Algorithm to produce Optimal Bus Stop Placement'})

        doc.wasAssociatedWith(get_NewStops, this_script)
        doc.used(get_NewStops, BusRoutes, startTime);
        doc.used(get_NewStops, Stations, startTime);
        doc.wasAttributedTo(NewStops, this_script);
        doc.wasGeneratedBy(NewStops, get_NewStops);
        doc.wasDerivedFrom(NewStops, BusRoutes);
        doc.wasDerivedFrom(NewStops, Stations);

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

    def run(trial = False):
        '''
        Scrap datasets and write provenance files
        set v = True for visualizations
        '''
        times = optimizeBusStops.execute(trial = trial)
        optimizeBusStops.provenance(startTime = times['start'], endTime = times['end'])

#if __name__ == '__main__':
#    optimizeBusStops.run()
# ## eof
