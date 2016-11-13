import json
import dml
import prov.model
import datetime
import uuid
import geocoder
from collections import Counter
import pandas as pd
import numpy as np 
from bs4 import BeautifulSoup
import urllib.request
import re
import itertools
import collections

class zipcodeRatings(dml.Algorithm):
    contributor = 'ktan_ngurung_yazhang_emilyh23'
    reads = ['ktan_ngurung_yazhang_emilyh23.tRidershipLocation', 'ktan_ngurung_yazhang_emilyh23.hubwayBigBellyCounts', 'ktan_ngurung_yazhang_emilyh23.collegeBusStopCounts']
    writes = ['ktan_ngurung_yazhang_emilyh23.zipcodeRatings']

    @staticmethod
    def get_rating(l, ct, z, star):
        mean = np.mean(l, axis=None)
        std = np.std(l, axis=0)
        high = mean + std/2
        low = mean - std/2
        temp = {} 
        if ct > high: 
            temp = {'zc': z, star: 3}
        elif ct < low: 
            temp = {'zc': z, star: 1}
        else: 
            temp = {'zc': z, star: 2}
        return temp 

    @staticmethod
    def get_overall(l, z, ct, star):
        mean = np.mean(l, axis=None)
        std = np.std(l, axis=0)
        high = mean + std/1.5
        low = mean - std/1.5
        temp = {} 
        if ct > high: 
            temp = {'zc': z, star: 3}
        elif ct < low: 
            temp = {'zc': z, star: 1}
        else: 
            temp = {'zc': z, star: 2}
        return temp

    @staticmethod
    def scrapeData(soup, zc_list):
        zc_land_area = {}
        for zc in zc_list:
            # 02446 is missing
            try: 
                zc_data_block = soup.find('div', {'id': zc})
                b_text = zc_data_block.findAll(text=True)
                land_area_index = b_text.index('Land area:')
                land_area = float(b_text[land_area_index+1])
                zc_land_area[zc] = land_area * 100
            except AttributeError:
                if zc == '02446':
                    zc_land_area[zc] = 129
        return zc_land_area

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung_yazhang_emilyh23', 'ktan_ngurung_yazhang_emilyh23')

        #Get bus stop and college location complete data
        hbbCounts = repo.ktan_ngurung_yazhang_emilyh23.hubwayBigBellyCounts.find_one()
        cbsCounts = repo.ktan_ngurung_yazhang_emilyh23.collegeBusStopCounts.find_one()
        tRideCounts = repo.ktan_ngurung_yazhang_emilyh23.tRidershipLocation.find_one()  

        # #3b. Capability to run algorithm in trial mode
        # if (trial == True):
        #     #When Trial Mode == True, this is the sample data
        #     hbbCounts = dict(list(itertools.islice(hbbCounts.items(), 0, 10)))
        #     print("SAMPLE")
        #     print(hbbCounts)
        #     cbsCounts = dict(list(itertools.islice(cbsCounts.items(), 0, 10)))
        #     print("SAMPLE")
        #     print(cbsCounts)
        #     tRideCounts = dict(list(itertools.islice(tRideCounts.items(), 0, 10)))
        #     print("SAMPLE")
        #     print(tRideCounts)


        hbbCbsDict = {} 
        tRideTransformed = [] 

        hbbCounts.pop('_id')
        cbsCounts.pop('_id')

        hbb = [] 
        for row in hbbCounts: 
            d = {'zc': row, 'bigBellyCount': hbbCounts[row]['bigBellyCount'], 'hubwayCount': hbbCounts[row]['hubwayCount']}
            hbb.append(d)
        
        hbb_df = pd.DataFrame(hbb)

        cbs = [] 
        for row in cbsCounts: 
            d = {'zc': row, 'busStopCount': cbsCounts[row]['busStopCount'], 'collegeCount': cbsCounts[row]['collegeCount']}
            cbs.append(d)

        cbs_df = pd.DataFrame(cbs)

        merged_df = pd.merge(cbs_df, hbb_df, on='zc') 

        # Transformed tRideCounts to dataframe
        for s in tRideCounts:
            try:
                temp = {'zc': tRideCounts[s]['zipcode'], 'loc':s, 'entry':tRideCounts[s]['entries']}
                tRideTransformed.append(temp)
            except TypeError:
                pass

        rider_df = pd.DataFrame(tRideTransformed)

        all_merged = pd.merge(merged_df, rider_df, on='zc')

        zc = set(all_merged['zc'])
        url = 'http://www.city-data.com/zipmaps/Boston-Massachusetts.html'
        r = urllib.request.urlopen(url)
        soup = BeautifulSoup(r, 'html.parser')
        zc_land_area = zipcodeRatings.scrapeData(soup, zc)

        # Lists for individual dictionary per zip code 
        bs_list = []  
        c_list = []  
        bb_list = []  
        h_list = []  
        ws_list = [] 

        # Dictionaries of each zipcode for the five criteria
        bs_d = {}   
        c_d = {} 
        bb_d = {} 
        h_d = {} 
        ws_d = {}

        # Values for standard deviation later 
        bs = []
        c = []
        bb = []
        h = []
        ws = []

        # For each zipcode, get a list of the values of the criteria and build a 
        # dictionary to associate each value with the corresponding zipcode 
        for z in zc: 
            z_df = all_merged.loc[all_merged['zc'] == z] 

            num_stations = len(z_df['entry'])
            entry_sum = sum(z_df['entry'])
            ws.append(num_stations * entry_sum)
            ws_d[z] = num_stations * entry_sum

            bs.append(z_df['busStopCount'].iloc[0] / zc_land_area[z])
            bs_d[z] = z_df['busStopCount'].iloc[0] / zc_land_area[z]

            c.append(z_df['collegeCount'].iloc[0] / zc_land_area[z])
            c_d[z] = z_df['collegeCount'].iloc[0] / zc_land_area[z]

            bb.append(z_df['bigBellyCount'].iloc[0] / zc_land_area[z])
            bb_d[z] = z_df['bigBellyCount'].iloc[0] / zc_land_area[z]

            h.append(z_df['hubwayCount'].iloc[0] / zc_land_area[z])
            h_d[z] = z_df['hubwayCount'].iloc[0] / zc_land_area[z]

        bs = sorted(bs)
        c = sorted(c)
        bb = sorted(bb)
        h = sorted(h)
        ws = sorted(ws)

        # For each zipcode, take the value of each criteria and pass to get_rating which calculates the standard 
        # deviation and assigns the appropriate rating based on the criteria's value
        for z in zc: 
            bs_ct = bs_d[z]
            bus_star = zipcodeRatings.get_rating(bs, bs_ct, z, 'bus_star')
            bs_list.append(bus_star)

            h_ct = h_d[z]
            hubway_star = zipcodeRatings.get_rating(h, h_ct, z, 'hubway_star')
            h_list.append(hubway_star)

            c_ct = c_d[z]
            college_star = zipcodeRatings.get_rating(c, c_ct, z, 'college_star')
            c_list.append(college_star)

            bb_ct = bb_d[z]
            bigBelly_star = zipcodeRatings.get_rating(bb, bb_ct, z, 'bigBelly_star')
            bb_list.append(bigBelly_star)

            ws_ct = ws_d[z] 
            ws_star = zipcodeRatings.get_rating(ws, ws_ct, z, 'station_star')
            ws_list.append(ws_star)

        bs_df = pd.DataFrame(bs_list)
        h_df = pd.DataFrame(h_list)
        c_df = pd.DataFrame(c_list) 
        bb_df = pd.DataFrame(bb_list)
        ws_df = pd.DataFrame(ws_list) 

        merge1_df = pd.merge(bs_df, h_df, on='zc')
        merge2_df = pd.merge(c_df, bb_df,on='zc') 
        merge3_df = pd.merge(merge1_df, merge2_df, on='zc')
        star_df = pd.merge(merge3_df, ws_df, on='zc') 

        overall_l = [] 
        overall_values = [] 

        # For each zipcode, find the weighted rating based on the criterias' rating and the criterias' counts 
        for z in zc: 
            z_df = star_df.loc[star_df['zc'] == z]
            bs_star = z_df['bus_star']
            h_star = z_df ['hubway_star']
            c_star = z_df['college_star']
            bb_star = z_df['bigBelly_star']
            ws_star = z_df['station_star']

            bs_weight = bs_star * bs_d[z]
            h_weight = h_star * h_d[z]
            c_weight = c_star * c_d[z]
            bb_weight = bb_star * bb_d[z]
            ws_weight = ws_star * ws_d[z]

            overall = int((bs_weight + h_weight + c_weight + bb_weight + ws_weight) / 5) 
            overall_values.append(overall)
            overall_l.append(overall)

        # Take each of the weighted ratings and pass to get_overall to get the final scaled rating 
        overall_dict = [] 
        i = 0 
        for z in zc: 
            rating = zipcodeRatings.get_overall(overall_l, z, overall_values[i], 'overall_star') 
            overall_dict.append(rating)
            i += 1 

        overall_dict = pd.DataFrame(overall_dict)
        star_df_final = pd.merge(star_df, overall_dict, on='zc')
        star_df_final.set_index('zc', drop=True, inplace=True)
        star_dict_final = star_df_final.to_dict(orient='index')
        print(star_df_final)

        for k, v in star_dict_final.items():
            star_dict_final[k]['hubway_star'] = int(star_dict_final[k]['hubway_star'])
            star_dict_final[k]['college_star'] = int(star_dict_final[k]['college_star'])
            star_dict_final[k]['bus_star'] = int(star_dict_final[k]['bus_star'])
            star_dict_final[k]['station_star'] = int(star_dict_final[k]['station_star'])
            star_dict_final[k]['bigBelly_star'] = int(star_dict_final[k]['bigBelly_star'])
            star_dict_final[k]['overall_star'] = int(star_dict_final[k]['overall_star'])

        # Convert dictionary into JSON object 
        data = json.dumps(star_dict_final, sort_keys=True, indent=2)
        r = json.loads(data)

        # Create new dataset called tRidershipLocation
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

zipcodeRatings.execute() 
# doc = zipcodeRatings.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
