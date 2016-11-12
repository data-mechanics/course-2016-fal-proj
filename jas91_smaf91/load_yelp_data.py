import uuid
import json
import dml
import prov.model
import datetime
import uuid

TRIAL_LIMIT = 5000

def load_business(repo):
    
    print("[OUT] Loading businesses information")

    filename = "../yelp/yelp_academic_dataset_business.json"
    dataset = "yelp_business"
    repo.dropPermanent(dataset)
    repo.createPermanent(dataset)

    for line in open(filename):
        doc = json.loads(line)
        r = {
            'businessname': doc['name'].lower(),
            'business_id': doc['business_id'],
            'geo_info': {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [float(doc['latitude']), float(doc['longitude'])]
                }
            }
        }
        repo['jas91_smaf91.' + dataset].insert_one(r)
        
    print('[OUT] Indexing by latitude and longitude')
    repo['jas91_smaf91.'+dataset].ensure_index([('geo_info.geometry', dml.pymongo.GEOSPHERE)])

def load_reviews(repo, trial):
    
    print("[OUT] Loading reviews")

    filename = "../yelp/yelp_academic_dataset_review.json"
    dataset = "yelp_reviews"
    repo.dropPermanent(dataset)
    repo.createPermanent(dataset)

    count = 0
    for line in open(filename):
        doc = json.loads(line)
        repo['jas91_smaf91.' + dataset].insert_one(doc)
        count += 1
        if trial and count > TRIAL_LIMIT:
            break

    print('[OUT] Indexing by business_id')
    repo.jas91_smaf91.yelp_reviews.create_index('business_id')

class load_yelp_data(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = ['yelp_academic_dataset_business', 'yelp_academic_dataset_review']
    writes = ['jas91_smaf91.yelp_business', 'jas91_smaf91.yelp_reviews']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        if trial:
            print("[OUT] Running in Trial Mode")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        load_business(repo)

        load_reviews(repo, trial)

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
        
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jas91_smaf91#load_yelp_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        load = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label': 'Load Yelp Business and Reviews'})
        doc.wasAssociatedWith(load, this_script)

        resource_yelp_academic_dataset_business = doc.entity('dat:jas91_smaf91#yelp_academic_dataset_business', {'prov:label':'Yelp Academic Dataset Business', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_yelp_academic_dataset_review = doc.entity('dat:jas91_smaf91#yelp_academic_dataset_review', {'prov:label':'Yelp Academic Dataset Review', prov.model.PROV_TYPE:'ont:DataSet'})
        yelp_business = doc.entity('dat:jas91_smaf91#yelp_business', {'prov:label':'Yelp Business Information', prov.model.PROV_TYPE:'ont:DataSet'})
        yelp_reviews = doc.entity('dat:jas91_smaf91#yelp_reviews', {'prov:label':'Yelp Reviews Information', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.usage( load, resource_yelp_academic_dataset_business, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage( load, resource_yelp_academic_dataset_review, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        doc.wasAttributedTo(yelp_business, this_script)
        doc.wasAttributedTo(yelp_reviews, this_script)
        
        doc.wasGeneratedBy(yelp_business, load, endTime)
        doc.wasGeneratedBy(yelp_reviews, load, endTime)
        
        doc.wasDerivedFrom(yelp_business, resource_yelp_academic_dataset_business, load, load, load) 
        doc.wasDerivedFrom(yelp_reviews, resource_yelp_academic_dataset_review, load, load, load) 
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

'''
load_yelp_data.execute(True)
doc = load_yelp_data.provenance()
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''


