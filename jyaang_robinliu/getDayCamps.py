import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class getDayCamps(dml.Algorithm):
    contributor = 'jyaang_robinliu106'
    reads = []
    writes = ['jyaang_robinliu106.dayCamp']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        url = "https://data.cityofboston.gov/api/views/sgf2-btru/rows.json?accessType=DOWNLOAD"
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        dayCampData = r['data']

        a = []
        for dayCamp in dayCampData:
            a.append([dayCamp[8],dayCamp[-1]])
        #delete last, empty entry
        a = a[:-1]

        d = []
        for entry in a:
            address = repr(entry[1][0])
            address = address[1:-1].split(',')
            if len(address[0]) < 3:
                continue
            city = str(address[1]).split(':')[1]
            city = city.strip("\"")
            d.append( {"name": entry[0], "city": city.replace('\\"',"\""), "coord" : entry[1][1:3]})
        #d = d.replace('\\"',"\"")

        repo.dropPermanent("dayCamp")
        repo.createPermanent("dayCamp")
        repo['jyaang_robinliu106.dayCamp'].insert_many(d)

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
        repo.authenticate('jyaang_robinliu106', 'jyaang_robinliu106')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jyaang_robinliu106#getDayCamps', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'dayCamp Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_dayCamp = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_dayCamp, this_script)
        doc.usage(get_dayCamp, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?type=dayCamp&$select=dayCampName,city,coord'
                }
            )

        dayCamp = doc.entity('dat:jyaang_robinliu106#dayCamp', {prov.model.PROV_LABEL:'dayCamp Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(dayCamp, this_script)
        doc.wasGeneratedBy(dayCamp, get_dayCamp, endTime)
        doc.wasDerivedFrom(dayCamp, resource, get_dayCamp, get_dayCamp, get_dayCamp)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

getDayCamps.execute()
doc = getDayCamps.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
