'''
CS 591 Project One
projOne.py
jzhou94@bu.edu
katerin@bu.edu
'''
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

def coordinatesToZip(lat, lan):
    ''' Converts lattitude and longitude into a zip code.
    '''
    api_key = 'AIzaSyAlEXXeSW2AWoCC1QDAfEjBC1OGc1yQgGU'
    url = 'https://maps.googleapis.com/maps/api/geocode/json?latlng=' + \
               str(lat) + ',' + str(lan) + '&key=' + api_key
    response = urllib.request.urlopen(url).read().decode("utf-8")
    data = json.loads(response)
    zip_code = (data['results'][0]['address_components'][7]['long_name'])
    return zip_code

class getLightsCoordintaes(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = []
    writes = ['jzhou94_katerin.lights_coordinates']

    @staticmethod
    def execute(trial = True):
        print("starting data retrieval")
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        ''' Street Light Locations '''
        token = ''
        url = 'https://data.cityofboston.gov/resource/fbdp-b7et.json?$$app_token=' + token + '&$limit=100000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        i = 0
        for item in r:
            lat = item['lat']
            long = item['long']
            item['zip'] = coordinatesToZip(long, lat)
            if i == 1000:
                break
            i += 1
        
        
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("lights_coordinates")
        repo.createPermanent("lights_coordinates")
        repo['jzhou94_katerin.lights_coordinates'].insert_many(r)
    
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
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jzhou94_katerin/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/jzhou94_katerin/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:getLightsCoordinates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_lights = doc.entity('bdp:fbdp-b7et', {'prov:label':'Street Light Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_lights = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_lights, this_script)
        doc.usage(get_lights, resource_lights, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?compnos,lat,long,objectid,the_geom,type'
                }
            )

        lights = doc.entity('dat:lights', {prov.model.PROV_LABEL:'Street Light Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lights, this_lights)
        doc.wasGeneratedBy(lights, get_lights, endTime)
        doc.wasDerivedFrom(lights, resource_lights, get_lights, get_lights, get_lights)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

getLightsCoordintaes.execute()
print("getLightsCoordintaes Algorithm Done")
doc = getLightsCoordintaes.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
print("getLightsCoordintaes Provenance Done")

## eof
