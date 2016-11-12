import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import numpy as np
from scipy.stats.stats import pearsonr
from collections import OrderedDict
import matplotlib.pyplot as plt
import seaborn as sns

class statCorrelation(dml.Algorithm):
    contributor = 'ktan_ngurung_yazhang_emilyh23'
    reads = ['ktan_ngurung_yazhang_emilyh23.zipcodeRatings']
    writes = ['ktan_ngurung_yazhang_emilyh23.correlation']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')

        # Get bus stop and college location data
        zipcode_ratings = repo.ktan_ngurung_yazhang_emilyh23.zipcodeRatings.find_one() 
        zipcode_ratings_df = pd.DataFrame(zipcode_ratings)
        del zipcode_ratings_df['_id']
        zipcode_ratings_df = zipcode_ratings_df.transpose()

        bigBelly_star = list(zipcode_ratings_df['bigBelly_star'])
        bus_star = list(zipcode_ratings_df['bus_star'])
        college_star = list(zipcode_ratings_df['college_star'])
        hubway_star = list(zipcode_ratings_df['hubway_star'])

        master_dict = {'bigBelly_star':bigBelly_star, 'bus_star':bus_star, 'college_star':college_star, 'hubway_star':hubway_star}
        bigBelly_cc, bus_cc, college_cc, hubway_cc = {}, {}, {}, {}
        print(master_dict)
        # Calculate the Pearson product-moment correlation coefficient for each category to every other category 

        # bigBelly_cc['bus_cc'] = pearsonr(master_dict['bigBelly_star'], master_dict['bigBelly_star'])
        # bigBelly_cc['college_cc'] = pearsonr(master_dict['college_star'], master_dict['bigBelly_star'])
        # bigBelly_cc['hubway_cc'] = pearsonr(master_dict['hubway_star'], master_dict['bigBelly_star'])
        # print('bigBelly_cc', bigBelly_cc)

        # bus_cc['college_cc'] = pearsonr(master_dict['college_star'], master_dict['bus_star'])
        # bus_cc['hubway_cc'] = pearsonr(master_dict['hubway_star'], master_dict['bus_star'])
        # print('bus_cc', bus_cc)

        # college_cc['hubway_cc'] = pearsonr(master_dict['hubway_star'], master_dict['college_star'])
        # print('college_cc', college_cc)

        # bigBelly_star_dict = OrderedDict(zipcode_ratings_df['bigBelly_star'])
        # bus_star_dict = OrderedDict(zipcode_ratings_df['bus_star'])

        # colors = np.random.rand(22)

        # plt.figure(1)
        # plt.subplot(211)
        # plt.scatter(range(len(bigBelly_star_dict.keys())), [int(v) for v in bigBelly_star_dict.values()], c=colors)
        # plt.xticks(range(len(bigBelly_star_dict.keys())))

        # plt.subplot(212)
        # plt.scatter(range(len(bus_star_dict.keys())), [int(v) for v in bus_star_dict.values()], c=colors)
        # plt.xticks(range(len(bus_star_dict.keys())))

        # plt.show()

        # Convert dictionary into JSON object 
        # data = json.dumps(star_dict_final, sort_keys=True, indent=2)
        # r = json.loads(data)

        # # Create new dataset called tRidershipLocation
        # repo.dropPermanent("zipcodeRatings")
        # repo.createPermanent("zipcodeRatings")
        # repo['ktan_ngurung_yazhang_emilyh23.zipcodeRatings'].insert_one(r)

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
        repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:ktan_ngurung_yazhang_emilyh23#merge', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        collegeBusStops_resource = doc.entity('dat:ktan_ngurung_yazhang_emilyh23/collegeBusStopCounts', {'prov:label':'Number of Colleges And Bus Stops for Each Zip code', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        tRidershipLocation_resource = doc.entity('dat:ktan_ngurung_yazhang_emilyh23/tRidershipLocation', {'prov:label':'Number of Entries for Each Train Location', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        hubwayBigBelly_resource = doc.entity('dat:ktan_ngurung_yazhang_emilyh23/hubwayBigBelly', {'prov:label':'Number of Hubways and Big Belly for Each Zip code', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, collegeBusStops_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )
        doc.usage(this_run, tRidershipLocation_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )
        doc.usage(this_run, hubwayBigBelly_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        zipcode_ratings = doc.entity('dat:ktan_ngurung_yazhang_emilyh23#zipcode-ratings', {prov.model.PROV_LABEL:'Critera Rating and Overall Rating for Zipcodes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(zipcode_ratings, this_script)
        doc.wasGeneratedBy(zipcode_ratings, this_run, endTime)
        doc.wasDerivedFrom(zipcode_ratings, collegeBusStops_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(zipcode_ratings, tRidershipLocation_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(zipcode_ratings, hubwayBigBelly_resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

statCorrelation.execute() 
# doc = zipcodeRatings.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof