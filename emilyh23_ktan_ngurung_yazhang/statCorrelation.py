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
from matplotlib import gridspec
import seaborn as sns
import warnings
import numpy 
warnings.filterwarnings("ignore")

class statCorrelation(dml.Algorithm):

    contributor = 'ktan_ngurung_yazhang_emilyh23'
    reads = ['ktan_ngurung_yazhang_emilyh23.zipcodeRatings']
    writes = ['ktan_ngurung_yazhang_emilyh23.statCorrelation'] 

    @staticmethod
    def execute():
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
        station_star = list(zipcode_ratings_df['station_star'])

        master_dict = {'bigBelly_star':bigBelly_star, 'bus_star':bus_star, 'college_star':college_star, 'hubway_star':hubway_star, 'station_star':station_star}
        bigBelly_cc, bus_cc, college_cc, hubway_cc, station_cc = {}, {}, {}, {}, {}

        # Calculate the Pearson product-moment correlation coefficient for each category to every other category 

        bigBelly_cc['bus_cc'] = pearsonr(master_dict['bus_star'], master_dict['bigBelly_star'])
        bigBelly_cc['college_cc'] = pearsonr(master_dict['college_star'], master_dict['bigBelly_star'])
        bigBelly_cc['hubway_cc'] = pearsonr(master_dict['hubway_star'], master_dict['bigBelly_star'])
        bigBelly_cc['station_cc'] = pearsonr(master_dict['station_star'], master_dict['bigBelly_star'])

        bus_cc['college_cc'] = pearsonr(master_dict['college_star'], master_dict['bus_star'])
        bus_cc['hubway_cc'] = pearsonr(master_dict['hubway_star'], master_dict['bus_star'])
        bus_cc['station_cc'] = pearsonr(master_dict['station_star'], master_dict['bus_star'])

        college_cc['hubway_cc'] = pearsonr(master_dict['hubway_star'], master_dict['college_star'])
        college_cc['station_cc'] = pearsonr(master_dict['station_star'], master_dict['college_star'])

        station_cc['hubway_cc'] = pearsonr(master_dict['hubway_star'], master_dict['station_star'])

        statCorrelation_dict = {'bigBelly_correlations':{}, 'bus_correlations':{}, 'college_correlations':{}, 'hubway_correlations':{}, 'train_correlations':{}}
        statCorrelation_dict['bigBelly_correlations'] = {'bus':bigBelly_cc['bus_cc'], 'college':bigBelly_cc['college_cc'], 'hubway':bigBelly_cc['hubway_cc'], 'train':bigBelly_cc['station_cc']}
        statCorrelation_dict['bus_correlations'] = {'bigBelly':bigBelly_cc['bus_cc'], 'college':bus_cc['college_cc'], 'hubway':bus_cc['hubway_cc'], 'train':bus_cc['station_cc']}
        statCorrelation_dict['college_correlations'] = {'bus':bus_cc['college_cc'], 'bigBelly':bigBelly_cc['college_cc'], 'hubway':college_cc['hubway_cc'], 'train':college_cc['station_cc']}
        statCorrelation_dict['hubway_correlations'] = {'bus':bus_cc['hubway_cc'], 'college':college_cc['hubway_cc'], 'bigBelly':bigBelly_cc['hubway_cc'], 'train':station_cc['hubway_cc']}
        statCorrelation_dict['train_correlations'] = {'bus':bigBelly_cc['station_cc'], 'college':college_cc['station_cc'], 'hubway':station_cc['hubway_cc'], 'bigBelly':bigBelly_cc['station_cc']}

        # Convert dictionary into JSON object 
        data = json.dumps(statCorrelation_dict, sort_keys=True, indent=2)
        r = json.loads(data)

        # Create new dataset called statCorrelation
        repo.dropPermanent("statCorrelation")
        repo.createPermanent("statCorrelation")
        repo['ktan_ngurung_yazhang_emilyh23.statCorrelation'].insert_one(r)

        # Plot the ratings for each zipcode to visualize any correlation

        bigBelly_star_dict = OrderedDict(zipcode_ratings_df['bigBelly_star'])
        bus_star_dict = OrderedDict(zipcode_ratings_df['bus_star'])
        college_star_dict = OrderedDict(zipcode_ratings_df['college_star'])
        hubway_dict = OrderedDict(zipcode_ratings_df['hubway_star'])
        station_dict = OrderedDict(zipcode_ratings_df['station_star'])

        colors = np.random.rand(22)

        fig = plt.figure()
        gs = gridspec.GridSpec(5, 2)

        ax1 = fig.add_subplot(gs[0,:])
        ax1.set_title('Big Belly Star Ratings')
        ax1.scatter(range(len(bigBelly_star_dict.keys())), [int(v) for v in bigBelly_star_dict.values()], c=colors)
        plt.xticks(range(len(bigBelly_star_dict.keys())))

        ax2 = fig.add_subplot(gs[1,:])
        ax2.set_title('Bus Star Ratings')
        ax2.scatter(range(len(bus_star_dict.keys())), [int(v) for v in bus_star_dict.values()], c=colors)
        plt.xticks(range(len(bus_star_dict.keys())))

        ax3 = fig.add_subplot(gs[2,:])
        ax3.set_title('College Star Ratings')
        ax3.scatter(range(len(college_star_dict.keys())), [int(v) for v in college_star_dict.values()], c=colors)
        plt.xticks(range(len(college_star_dict.keys())))

        ax4 = fig.add_subplot(gs[3,:])
        ax4.set_title('Hubway Star Ratings')
        ax4.scatter(range(len(hubway_dict.keys())), [int(v) for v in hubway_dict.values()], c=colors)
        plt.xticks(range(len(hubway_dict.keys())))

        ax5 = fig.add_subplot(gs[4,:])
        ax5.set_title('Station Star Ratings')
        ax5.scatter(range(len(station_dict.keys())), [int(v) for v in station_dict.values()], c=colors)
        plt.xticks(range(len(station_dict.keys())))

        plt.tight_layout()
        plt.show()

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

        this_script = doc.agent('alg:ktan_ngurung_yazhang_emilyh23#statCorrelation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        zipcodeRatings_resource = doc.entity('dat:ktan_ngurung_yazhang_emilyh23/zipcode-ratings', {'prov:label':'Critera Rating and Overall Rating for Zipcodes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, zipcodeRatings_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        statCorrelation = doc.entity('dat:ktan_ngurung_yazhang_emilyh23#statCorrelation', {prov.model.PROV_LABEL:'Correlation Coefficient of Every Criteria to Each Other', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(statCorrelation, this_script)
        doc.wasGeneratedBy(statCorrelation, this_run, endTime)
        doc.wasDerivedFrom(statCorrelation, zipcodeRatings_resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

statCorrelation.execute()
doc = statCorrelation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof