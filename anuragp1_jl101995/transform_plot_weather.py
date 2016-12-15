
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics
import pandas as pd
from bson.code import Code
import matplotlib.pyplot as plt
import pylab
import seaborn as sns


class plot_weather(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = ['anuragp1_jl101995.citibike_weather', 'anuragp1_jl101995.turnstile_weather']
    writes = []

    @staticmethod
    def execute(Trial=False):
        '''Retrieve some datasets'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        # When Trial is True, perform function on random sample of size SIZE)
        SIZE = 100

        def scaleEntry(OldValue):
            OldMax = 800000
            OldMin = 100000
            NewMax = 200
            NewMin = 100
            OldRange = (OldMax - OldMin)  
            NewRange = (NewMax - NewMin)  
            NewValue = (((OldValue - OldMin) * NewRange) / OldRange) + NewMin
            return NewValue

        def scalePrecip(OldValue):
            OldMax = 10
            OldMin = 0
            NewMax = 100
            NewMin = 0
            OldRange = (OldMax - OldMin)  
            NewRange = (NewMax - NewMin)  
            NewValue = (((OldValue - OldMin) * NewRange) / OldRange) + NewMin
            return NewValue


        # print('Loading in turnstile_weather from Mongo')

        # tw_data = repo.anuragp1_jl101995.turnstile_weather.find()
        # data =[]
        # for entry in tw_data:
        #     data.append((scalePrecip(entry['Precip']),entry['AvgTemp'] , scaleEntry(entry['Entries']), entry['Date']))
            
        # tw_df = pd.DataFrame(data, columns = ['Precip', 'Temp', 'Entries', 'Date'])

        # print('Finished')



        # ax = tw_df.plot(x='Date', y=['Entries','Temp' ,'Precip'])
        # ax.locator_params(axis='x',nbins=6)
        # ax.axes.get_yaxis().set_ticks([])
        # print('Create turnstile_temp_precip.png')
        # ax.savefig('turnstile_temp_precip.png')



        # def scaleCiti(OldValue):
        #     OldMax = 40000
        #     OldMin = 1000
        #     NewMax = 200
        #     NewMin = 100
        #     OldRange = (OldMax - OldMin)  
        #     NewRange = (NewMax - NewMin)  
        #     NewValue = (((OldValue - OldMin) * NewRange) / OldRange) + NewMin
        #     return NewValue

        # print('Loading in citibike_weather from Mongo')
        # cw_data = repo.anuragp1_jl101995.citibike_weather.find()
        # data =[]
        # for entry in cw_data:
        #     data.append((scalePrecip(entry['Precip']),entry['AvgTemp'] , scaleCiti(entry['Citibike_Usage']), entry['Date']))
        # cw_df = pd.DataFrame(data, columns = ['Precip', 'Temp', 'Citi_Use', 'Date'])




        # ax = cw_df.plot(x='Date', y=['Citi_Use','Temp' ,'Precip'])
        # ax.axes.get_yaxis().set_ticks([])
        # print('Create citibike_temp_precip.png')
        # ax.savefig('citibike_temp_precip.png')





        # Plotting CitiBike usage and weather
        print('Loading in citibike_weather from Mongo')

        cw_noscale_data = repo.anuragp1_jl101995.citibike_weather.find()
        data =[]
        for entry in cw_noscale_data:
            data.append(((entry['Precip']),entry['AvgTemp'] , (entry['Citibike_Usage']), entry['Date']))
        cw_noscale_df = pd.DataFrame(data, columns = ['Precip', 'Temp', 'Citi_Use', 'Date'])

        # Create scatterplot with regression line
        c = sns.regplot(x='Temp', y='Citi_Use', data=cw_noscale_df, ci = False, 
            scatter_kws={'color':'#066FD5','alpha':0.4,'s':80},
            line_kws={'color':'#066FD5','alpha':0.5,'lw':4},marker='x')

        # remove the top and right line in graph
        sns.despine()

        # Set graph size
        c.figure.set_size_inches(10,7)
        # Set graph title
        c.axes.set_title('CitiBike Usage by Temperature',color='black',fontsize=18,alpha=0.95)
        # Set xlabel
        c.set_xlabel(r'Temperature ($^\circ$F)',size = 16,color='black',alpha=1)
        # Set ylabel
        c.set_ylabel('Daily CitiBike Usage',size = 16,color='black',alpha=1)
        # Set ticklabel
        c.tick_params(labelsize=10,labelcolor='black')
        print('Create citibike_temp_regression.png')
        plt.savefig('citibike_temp_regression.png')
        plt.clf()





        # Plotting subway usage and weather
        print('Loading in turnstile_weather from Mongo')

        tw_noscale_data = repo.anuragp1_jl101995.turnstile_weather.find()
        tw_noscale_data
        data =[]
        for entry in tw_noscale_data:
            data.append(((entry['Precip']),entry['AvgTemp'], (entry['Entries']), entry['Date']))
        tw_noscale_df = pd.DataFrame(data, columns = ['Precip', 'Temp', 'Subway_Use', 'Date'])

        # Create scatterplot with regression line
        s = sns.regplot(x='Temp', y='Subway_Use', data=tw_noscale_df, ci = False, 
            scatter_kws={'color':'#FF5722','alpha':0.4,'s':80},
            line_kws={'color':'#FF5722','alpha':0.5,'lw':4},marker='x')

        # remove the top and right line in graph
        sns.despine()

        # Set graph size
        s.figure.set_size_inches(10,7)
        # Set graph title
        s.axes.set_title('Subway Usage by Temperature',color='black',fontsize=18,alpha=0.95)
        # Set xlabel
        s.set_xlabel(r'Temperature ($^\circ$F)',size = 16,color='black',alpha=1)
        # Set ylabel
        s.set_ylabel('Daily Subway Usage',size = 16,color='black',alpha=1)
        # Set ticklabel
        s.tick_params(labelsize=10,labelcolor='black')
        print('Create subway_temp_regression.png')
        plt.savefig('subway_temp_regression.png')

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cny', 'https://data.cityofnewyork.us/resource/') # NYC Open Data
        doc.add_namespace('mta', 'http://web.mta.info/developers/') # MTA Data (turnstile source)

        this_script = doc.agent('alg:anuragp1_jl101995#transformation5', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # Transform associating weather with turnstile
        turnstile_weather_resource = doc.entity('dat:subway_regions',{'prov:label':'Turnstile Weather Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_turnstile_weather = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_turnstile_weather, this_script)
        doc.usage(get_turnstile_weather, turnstile_weather_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'} )
        turnstile_weather = doc.entity('dat:anuragp1_jl101995#turnstile_weather', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(turnstile_weather, this_script)
        doc.wasGeneratedBy(turnstile_weather, get_turnstile_weather, endTime)
        doc.wasDerivedFrom(turnstile_weather, turnstile_weather_resource, get_turnstile_weather, get_turnstile_weather, get_turnstile_weather)

        # Subway Stations Data
        stations_resource = doc.entity('cny:subway_stations',{'prov:label':'Subway Stations Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_stations, this_script)
        doc.usage(get_stations, stations_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:DataSet'} )
        stations = doc.entity('dat:anuragp1_jl101995#subway_stations', {prov.model.PROV_LABEL:'NYC Subway Stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(stations, this_script)
        doc.wasGeneratedBy(stations, get_stations, endTime)
        doc.wasDerivedFrom(stations, stations_resource, get_stations, get_stations, get_stations)

        # Turnstile Data
        turnstile_resource = doc.entity('mta:turnstile', {'prov:label':'Turnstile Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        get_turnstile = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 
        doc.wasAssociatedWith(get_turnstile, this_script)
        doc.usage(get_turnstile, turnstile_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:DataSet'} )
        turnstile = doc.entity('dat:anuragp1_jl101995#turnstile', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(turnstile, this_script)
        doc.wasGeneratedBy(turnstile, get_turnstile, endTime)
        doc.wasDerivedFrom(turnstile, turnstile_resource, get_turnstile, get_turnstile, get_turnstile)


        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


plot_weather.execute(Trial=False)
doc = plot_weather.provenance()
