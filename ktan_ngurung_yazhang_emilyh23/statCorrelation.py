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

def statCorrelation():
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
    print('bigBelly_cc', bigBelly_cc)

    bus_cc['college_cc'] = pearsonr(master_dict['college_star'], master_dict['bus_star'])
    bus_cc['hubway_cc'] = pearsonr(master_dict['hubway_star'], master_dict['bus_star'])
    bus_cc['station_cc'] = pearsonr(master_dict['station_star'], master_dict['bus_star'])
    print('bus_cc', bus_cc)

    college_cc['hubway_cc'] = pearsonr(master_dict['hubway_star'], master_dict['college_star'])
    college_cc['station_cc'] = pearsonr(master_dict['station_star'], master_dict['college_star'])
    print('college_cc', college_cc)

    station_cc['hubway_cc'] = pearsonr(master_dict['hubway_star'], master_dict['station_star'])
    print('station_cc', station_cc)

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

statCorrelation()

## eof