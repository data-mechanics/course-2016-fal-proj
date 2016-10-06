import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class transformation1(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp1_jl101995.subway_stations', 'anuragp1_jl101995.pedestriancounts']
    writes = ['anuragp1_jl101995.subway_regions']

    @staticmethod
    def execute(Trial = False):
        '''Retrieve some data sets'''

        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        repo.anuragp1_jl101995.subway_stations.ensure_index( [( 'the_geom' ,dml.pymongo.GEOSPHERE )] )
        repo.anuragp1_jl101995.pedestriancounts.ensure_index( [('the_geom' ,dml.pymongo.GEOSPHERE )] )

        repo.dropPermanent("subway_regions")
        repo.createPermanent("subway_regions")

        for this_loc in repo.anuragp1_jl101995.subway_stations.find():
            closest_stations = repo.command(
                'geoNear','anuragp1_jl101995.pedestriancounts',
                near={
                'type' : 'Point', 
                'coordinates' : this_loc['the_geom']['coordinates']},
                spherical=True,
                maxDistance = 8000
                )['results']

            subway_regions = { 'Line' : this_loc['line'], 'Station_Name' : this_loc['name'], 'Closest_Region' :closest_stations[0]['obj']['loc']}
            repo['anuragp1_jl101995.subway_regions'].insert_one(subway_regions)

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
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cny', 'https://data.cityofnewyork.us/resource/') # NYC Open Data  

        this_script = doc.agent('alg:anuragp1_jl101995#transform1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # Transform associating each subway station with its pedestrian count
        subway_regions_resource = doc.entity('dat:subway_regions',{'prov:label':'Subway Station Region Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_subwayregions = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_subwayregions, this_script)
        doc.usage(get_subwayregions, subway_regions_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'} )
        subway_regions = doc.entity('dat:anuragp1_jl101995#subway_regions', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(subway_regions, this_script)
        doc.wasGeneratedBy(subway_regions, get_subwayregions, endTime)
        doc.wasDerivedFrom(subway_regions, subway_regions_resource, get_subwayregions, get_subwayregions, get_subwayregions) 

        # Subway Stations Data
        stations_resource = doc.entity('cny:subway_stations',{'prov:label':'Subway Stations Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stations, this_script)
        doc.usage(get_stations, stations_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:DataSet'} )
        stations = doc.entity('dat:anuragp1_jl101995#subway_stations', {prov.model.PROV_LABEL:'NYC Subway Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, get_stations, endTime)
        doc.wasDerivedFrom(stations, stations_resource, get_stations, get_stations, get_stations)

        # Pedestrians Count Data 
        pedestrian_resource = doc.entity('cny:pedestriancounts',{'prov:label':'Pedestrians Count Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_pedestrian = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_pedestrian, this_script)
        doc.usage(get_pedestrian, pedestrian_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:DataSet'} )
        pedestrian = doc.entity('dat:anuragp1_jl101995#pedestriancounts', {prov.model.PROV_LABEL:'NYC Bi-Annual Pedestrian Counts', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pedestrian, this_script)
        doc.wasGeneratedBy(pedestrian, get_pedestrian, endTime)
        doc.wasDerivedFrom(pedestrian, pedestrian_resource, get_pedestrian, get_pedestrian, get_pedestrian) 

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()
            
        return doc


transformation1.execute()
doc = transformation1.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof