import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class collectData(dml.Algorithm):
    contributor = 'arjunlam'
    reads = []
    writes = ['arjunlam.crime', 'arjunlam.closed311', 'arjunlam.develop', 'arjunlam.hotline', 'arjunlam.potholes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('arjunlam', 'arjunlam')

        #Crime incident report
        url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json?$limit=150000&$$app_token=lWIyWqpeAAochCKL22wq3DVpf&$order=occurred_on_date%20DESC'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropPermanent("crime")
        repo.createPermanent("crime")
        repo['arjunlam.crime'].insert_many(r)

        #311 Service requests (you can run this but it will take a while to download)
        url = 'https://data.cityofboston.gov/resource/wc8w-nujj.json?$limit=150000&$$app_token=lWIyWqpeAAochCKL22wq3DVpf&$order=open_dt%20DESC'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        
        url2 = 'https://data.cityofboston.gov/resource/wc8w-nujj.json?$$app_token=lWIyWqpeAAochCKL22wq3DVpf&$order=open_dt%20DESC&$limit=50000&$offset=150000'
        response2 = urllib.request.urlopen(url2).read().decode("utf-8")
        r2 = json.loads(response2)
        
        url3 = 'https://data.cityofboston.gov/resource/wc8w-nujj.json?$$app_token=lWIyWqpeAAochCKL22wq3DVpf&$order=open_dt%20DESC&$limit=50000&$offset=200000'
        response3 = urllib.request.urlopen(url3).read().decode("utf-8")
        r3 = json.loads(response3)
        
        url4 = 'https://data.cityofboston.gov/resource/wc8w-nujj.json?$$app_token=lWIyWqpeAAochCKL22wq3DVpf&$order=open_dt%20DESC&$limit=50000&$offset=250000'
        response4 = urllib.request.urlopen(url4).read().decode("utf-8")
        r4 = json.loads(response4)

        
        url5 = 'https://data.cityofboston.gov/resource/wc8w-nujj.json?$$app_token=lWIyWqpeAAochCKL22wq3DVpf&$order=open_dt%20DESC&$limit=50000&$offset=300000'
        response5 = urllib.request.urlopen(url5).read().decode("utf-8")
        r5 = json.loads(response5)

        
        repo.dropPermanent("closed311")
        repo.createPermanent("closed311")
        repo['arjunlam.closed311'].insert_many(r)
        repo['arjunlam.closed311'].insert_many(r2)
        repo['arjunlam.closed311'].insert_many(r3)
        repo['arjunlam.closed311'].insert_many(r4)
        repo['arjunlam.closed311'].insert_many(r5)


        #Department of development developed property
        url = 'https://data.cityofboston.gov/resource/k26b-7bmj.json?$limit=50000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        #s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("develop")
        repo.createPermanent("develop")
        repo['arjunlam.develop'].insert_many(r)
        
        #mayor hotline city services
        url = 'https://data.cityofboston.gov/resource/jbcd-dknd.json?$limit=50000&$$app_token=lWIyWqpeAAochCKL22wq3DVpf'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        #s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("hotline")
        repo.createPermanent("hotline")
        repo['arjunlam.hotline'].insert_many(r)
        
        #Closed potholes cases 17000
        url = 'https://data.cityofboston.gov/resource/wivc-syw7.json?$limit=50000&$$app_token=lWIyWqpeAAochCKL22wq3DVpf'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        #s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("potholes")
        repo.createPermanent("potholes")
        repo['arjunlam.potholes'].insert_many(r)
        
        
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
        repo.authenticate('arjunlam', 'arjunlam')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:arjunlam#collectData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        crime_entity = doc.entity('bdp:29yf-ye7n', {'prov:label':'Crime Incident Report', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        closed311_entity = doc.entity('bdp:wc8w-nujj', {'prov:label':'Closed 311 Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        develop_entity = doc.entity('bdp:k26b-7bmj', {'prov:label':'DND Developed Property', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        hotline_entity = doc.entity('bdp:jbcd-dknd', {'prov:label':'Mayor 24hr Hotline', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        pothole_entity = doc.entity('bdp:wivc-syw7', {'prov:label':'Closed Pothole Cases', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_closed311 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_develop = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_hotline = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_potholes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_closed311, this_script)
        doc.wasAssociatedWith(get_develop, this_script)
        doc.wasAssociatedWith(get_hotline, this_script)
        doc.wasAssociatedWith(get_potholes, this_script)
        
        doc.usage(get_crime, crime_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_closed311, closed311_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_develop, develop_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_hotline, hotline_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_potholes, pothole_entity, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
            
        crime = doc.entity('dat:arjunlam#crime', {prov.model.PROV_LABEL:'Crime Incident Report', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, crime_entity, get_crime, get_crime, get_crime)

        closed311 = doc.entity('dat:arjunlam#closed311', {prov.model.PROV_LABEL:'Closed 311', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(closed311, this_script)
        doc.wasGeneratedBy(closed311, get_closed311, endTime)
        doc.wasDerivedFrom(closed311, closed311_entity, get_closed311, get_closed311, get_closed311)
        
        develop = doc.entity('dat:arjunlam#develop', {prov.model.PROV_LABEL:'DND Developed Property', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(develop, this_script)
        doc.wasGeneratedBy(develop, get_develop, endTime)
        doc.wasDerivedFrom(develop, develop_entity, get_develop, get_develop, get_develop)
        
        hotline = doc.entity('dat:arjunlam#hotline', {prov.model.PROV_LABEL:'Mayor 24hr Hotline', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hotline, this_script)
        doc.wasGeneratedBy(hotline, get_hotline, endTime)
        doc.wasDerivedFrom(hotline, hotline_entity, get_hotline, get_hotline, get_hotline)
        
        potholes = doc.entity('dat:arjunlam#potholes', {prov.model.PROV_LABEL:'Closed Pothole Cases', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(potholes, this_script)
        doc.wasGeneratedBy(potholes, get_potholes, endTime)
        doc.wasDerivedFrom(potholes, pothole_entity, get_potholes, get_potholes, get_potholes)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc
        
collectData.execute()
#doc = collectData.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof