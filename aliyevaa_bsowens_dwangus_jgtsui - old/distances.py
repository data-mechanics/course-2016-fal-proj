import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
import ast
import math



def calculate(x0,y0,x1,y1):
    x_d_sq=pow(abs(x0)-abs(x1),2)
    y_d_sq=pow(abs(y0)-abs(y1),2)
    dist=math.sqrt(x_d_sq+y_d_sq)
    return dist

class distances(dml.Algorithm):
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'

    oldSetExtensions = ['community_indicators', 'cell_GPS_center_coordinates']
    titles = ['Boston Grid Cells Inverse Community Score']
    setExtensions = ['boston_grid_community_values_cellSize1000sqft']

    reads = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in oldSetExtensions]
    writes = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in setExtensions]

    dataSetDict = {}
    for i in range(len(setExtensions)):
        dataSetDict[setExtensions[i]] = (writes[i], titles[i])
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(distances.contributor, distances.contributor)

        repo.dropPermanent("boston_grid_community_values_cellSize1000sqft")
        repo.createPermanent("boston_grid_community_values_cellSize1000sqft")

        myrepo = repo.aliyevaa_bsowens_dwangus_jgtsui

        cellCoordinates = repo[distances.reads[1]]
        coordinates = []
        for cellCoord in cellCoordinates.find():
            coordinates.append((cellCoord['longitude'], cellCoord['latitude']))

        '''
        lines = [line.rstrip('\n') for line in open('centers.txt')]
        coordinates=[]
        point=[]
        for line in lines:
            if line!='':
                point=[float(x) for x in line.split() ]
                coordinates.append(point)
                point=[]

        out=open('out.txt', 'w')
        #'''
        score=0
        prep=[]

        for center in coordinates:
            lat = center[1]
            long = center[0]
            for elem in repo[distances.reads[0]].find():  # Every entry in community_indicators
                d = calculate(elem['location']['coordinates'][0], elem['location']['coordinates'][1], long,lat)
                score += d*elem['community_score']

            entry = {}
            entry.update({"cell_community_value": 1.0/score, \
                          "cell_center_latitude": lat, \
                          "cell_center_longitude": long})
            prep.append(entry)
            score=0

        str_prep=', '.join(json.dumps(d) for d in prep)
        l_prep='['+str_prep+']'
        r=json.loads(l_prep)
        myrepo["boston_grid_community_values_cellSize1000sqft"].insert_many(r)
        #json.dump(entry, out)
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        client =  dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(distances.contributor,distances.contributor)
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#distancesCommunityScoreCalculations',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_liquor_data, this_script)

        found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#boston_grid_community_values_cellSize1000sqft', {prov.model.PROV_LABEL:'calculating community values', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_liquor_data, endTime)
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()
        return doc

distances.execute()
doc = distances.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
