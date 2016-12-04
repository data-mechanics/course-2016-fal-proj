import uuid
import json
import dml
import prov.model
import datetime
import scipy
from scipy.stats import pearsonr
from matplotlib import pyplot as plt
import numpy as np
import sys
import warnings

TRIAL_LIMIT = 5000

def join_business_inspections(repo, trial):
    repo.dropPermanent('jas91_smaf91.yelp_business_inspections')
    repo.createPermanent('jas91_smaf91.yelp_business_inspections')
   
    if trial:
        records = repo.jas91_smaf91.food.find().limit(TRIAL_LIMIT)
    else:
        records = repo.jas91_smaf91.food.find()
    
    for document in records:

        latitude = document['geo_info']['geometry']['coordinates'][0]
        longitude = document['geo_info']['geometry']['coordinates'][1]

        if not longitude and not latitude:
            continue
        
        business = repo.jas91_smaf91.yelp_business.find({
            'businessname': document['businessname'].lower(),
        })
        
        if not business or business.count() == 0:
            continue
            
        if business.count() > 1:
            business = repo.jas91_smaf91.yelp_business.find_one({
                'businessname': document['businessname'].lower(),
                'geo_info.geometry': {
                    '$near': { 
                        '$geometry': {
                            'type': 'Point', 
                            'coordinates' :[latitude,longitude]
                            }, 
                        '$maxDistance': 50, 
                        '$minDistance': 0 
                        } 
                    }
                })
        else:
            business = business[0]
        
        if not business:
            continue


        document['business_id'] = business['business_id']
        repo.jas91_smaf91.yelp_business_inspections.insert_one(document)


def find_inspection_dates(repo): 
    for _id in repo.jas91_smaf91.yelp_business_inspections.distinct('business_id'):
        dates = {}
        for entry in repo.jas91_smaf91.yelp_business_inspections.find({'business_id': _id}).sort('resultdttm', 1):
            if entry['business_id'] not in BUSINESS:
                BUSINESS[entry['business_id']] = ['2000-12-31T23:59:00.000']
            
            if 'resultdttm' not in entry:
                continue 

            if entry['resultdttm'] not in dates:
                BUSINESS[entry['business_id']].append(entry['resultdttm'])
                dates[entry['resultdttm']] = 1


def inspection_score(repo, business_id, date):
    severity = [0,0,0,0]
    penalty = 0
    for inspection in repo.jas91_smaf91.yelp_business_inspections.find({'business_id': business_id, 'resultdttm':date }):
        if 'viollevel' not in inspection:
            continue
            
        level = inspection['viollevel'].replace(' ','')
        
        try:
            severity[len(level)] += 1
            penalty += len(level)
        except:
            pass
    return severity, penalty


def ratings_before_ispection(repo, business_id, date, previous_date):
    ratings = [0,0,0,0,0]

    for review in repo.jas91_smaf91.yelp_reviews.find({'business_id': business_id, 
        'date': {'$lte': date, '$gt': previous_date}}):
        stars = review['stars']
        ratings[stars-1] += 1
    
    avg = ((1*ratings[0]) + (2*ratings[1]) + (3*ratings[2]) + (4*ratings[3]) + (5*ratings[4])) / max(sum(ratings),1)

    return avg

def associate_reviews(repo):
    repo.dropPermanent('jas91_smaf91.yelp_business_inspections_summary')
    repo.createPermanent('jas91_smaf91.yelp_business_inspections_summary')
    
    for business_id in BUSINESS:
        dates = BUSINESS[business_id]
        for i in range(len(dates)-1):
            severity, penalty = inspection_score(repo, business_id, dates[i+1])
            avg_rating = ratings_before_ispection(repo, business_id, dates[i+1], dates[i])
            if avg_rating > 0:
                r = {'business_id': business_id, 
                     'date': dates[i+1], 
                     'avg_rating': avg_rating,
                     'level0': severity[0],
                     'level1': severity[1],
                     'level2': severity[2],
                     'level3': severity[3],
                     'penalty': penalty
                    }
                repo.jas91_smaf91.yelp_business_inspections_summary.insert_one(r)
                scores.append(sum(severity))
                severities.append(severity)
                ratings.append(avg_rating)

def evaluate_correlation(scores,ratings):
    if len(scores) > 1 and len(ratings) > 1:
        coef, p_value = pearsonr(scores, ratings)
        return (coef, p_value)
    else:
        print("[OUT] Pearson Correlation cannot be calculated: Not enough values. Try in non trial mode")

def ratings_by_severity(ratings, severities):
    level0 = []
    level1 = []
    level2 = []
    level3 = []
    penal_scores = []
    number_violations = []
    
    for i in range(len(ratings)):
        level0.append(severities[i][0])
        level1.append(severities[i][1])
        level2.append(severities[i][2])
        level3.append(severities[i][3])
        
        penal_scores.append((severities[i][1]*1) + (severities[i][2]*2) + (severities[i][3]*3))
        number_violations.append(sum(severities[i]))
        
    print("Feature", "Pearson Coef", "p-value")
    print("Level 1", evaluate_correlation(level1,ratings))
    print("Level 2", evaluate_correlation(level2,ratings))
    print("Level 3", evaluate_correlation(level3,ratings))
    print("Penalty Score", evaluate_correlation(penal_scores,ratings))
    print("Number of violations", evaluate_correlation(number_violations,ratings))

    #plt.scatter(penal_scores, ratings, s=1, alpha=1)
    #plt.xlabel("Penalty Score")
    #plt.ylabel("Average User Star Rating")
    #plt.show()

BUSINESS = {}
scores = []
ratings = []
severities = []

class rating_inspection_correlation(dml.Algorithm):
    contributor = 'jas91_smaf91'
    reads = []
    writes = []

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        if trial:
            print("[OUT] Running in Trial Mode")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jas91_smaf91', 'jas91_smaf91')

        join_business_inspections(repo, trial)
        find_inspection_dates(repo)
        associate_reviews(repo)
        ratings_by_severity(ratings,severities)

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

        this_script = doc.agent('alg:jas91_smaf91#rating_inspection_correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        analysis = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label': 'Correlation Analysis over Food Inspection and Yelp user ratings'})
        doc.wasAssociatedWith(analysis, this_script)
        
        resource_yelp_business = doc.entity('dat:jas91_smaf91#yelp_business', {'prov:label':'Yelp Business Information', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_yelp_reviews = doc.entity('dat:jas91_smaf91#yelp_reviews', {'prov:label':'Yelp Reviews Information', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_food = doc.entity('dat:jas91_smaf91#food', {'prov:label':'Food Establishment Inspections', prov.model.PROV_TYPE:'ont:DataSet'})
        
        yelp_business_inspections = doc.entity('dat:jas91_smaf91#yelp_business_inspections', {'prov:label':'Food Inspections with business_id from yelp business', prov.model.PROV_TYPE:'ont:DataSet'})
        yelp_business_inspections_summary = doc.entity('dat:jas91_smaf91#yelp_business_inspections_summary', {'prov:label':'Penalty scores and user ratings per inspection', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.usage( analysis, resource_yelp_business, startTime, None, {prov.model.PROV_TYPE:'ont:Query'})
        doc.usage( analysis, resource_food, startTime, None, {prov.model.PROV_TYPE:'ont:Query'})
        doc.usage( analysis, resource_yelp_reviews, startTime, None, {prov.model.PROV_TYPE:'ont:Query'})
        doc.usage( analysis, yelp_business_inspections, startTime, None, {prov.model.PROV_TYPE:'ont:Computation'})
        
        doc.wasAttributedTo(yelp_business_inspections, this_script)
        doc.wasAttributedTo(yelp_business_inspections_summary, this_script)

        doc.wasGeneratedBy(yelp_business_inspections, analysis, endTime)
        doc.wasGeneratedBy(yelp_business_inspections_summary, analysis, endTime)
        
        doc.wasDerivedFrom(yelp_business_inspections, resource_yelp_business, analysis, analysis, analysis) 
        doc.wasDerivedFrom(yelp_business_inspections, resource_food, analysis, analysis, analysis) 
        
        doc.wasDerivedFrom(yelp_business_inspections_summary, yelp_business_inspections, analysis, analysis, analysis) 
        doc.wasDerivedFrom(yelp_business_inspections_summary, resource_yelp_reviews, analysis, analysis, analysis) 
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

if 'trial' in sys.argv:
    rating_inspection_correlation.execute(True)
#else:
#    rating_inspection_correlation.execute()
#
#
#doc = rating_inspection_correlation.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))
