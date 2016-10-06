import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
import statistics

class transformation3(dml.Algorithm):
    contributor = 'anuragp1_jl101995'
    reads = [ 'anuragp1_jl101995.weather,' 'anuragp1_jl101995.turnstile']
    writes = ['anuragp1_jl101995.turnstiles_and_weather']

    @staticmethod
    def execute(Trial = False):
        '''Retrieve some datasets'''

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        weatherdata = repo.anuragp1_jl101995.weather.find()
        turnstiledata = repo.anuragp1_jl101995.turnstile.find()

        repo.dropPermanent('turnstiles_and_weather')
        repo.createPermanent('turnstiles_and_weather')
    
        # List of dictionary entries containing each day's {date, avg temp, precip}
        date_weather = []

        for w in weatherdata:
            int2string = str(int(w['DATE']))
            # Change weather date format from YYYYMMDD (int) => 'MM/DD/YYYY' (str) 
            datestring = int2string[4:6] + '/' + int2string[6:8] + '/' + int2string[0:4]

            # Calculate average temperature for the day
            avgtemp = statistics.mean([w['TMAX'], w['TMIN']])

            # Insert into our date
            insert_weather = {'Date': datestring, 'AvgTemp': avgtemp, 'Precip': w['PRCP']}
            date_weather.append(insert_weather)

        repo.dropPermanent('turnstiles_and_weather')
        repo.createPermanent('turnstiles_and_weather')

        for t in turnstiledata:
            for d in date_weather:

                if t['DATE'] == d['Date']: 
                    tw_fields = {'Date':d['Date'], 'Station':t['STATION'], 'Entries':t['ENTRIES'], \
                                 'Exits':t['EXITS                                                               '], 'LineName':t['LINENAME'], \
                                 'Time':t['TIME'], 'AvgTemp':d['AvgTemp'], 'Precip':d['Precip']} 
                    # insert this dictionary into the database
                    repo.anuragp1_jl101995.turnstiles_and_weather.insert_one(tw_fields)

        # end database connection
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
        repo.authenticate('anuragp1_jl101995', 'anuragp1_jl101995')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('mch', 'http://datamechanics.io/data/anuragp1_jl101995/')  # Data Mechanics S3 bucket (weather file source)
        doc.add_namespace('mta', 'http://web.mta.info/developers/') # MTA Data (turnstile source)

        this_script = doc.agent('alg:anuragp1_jl101995#transformation3', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        weather_resource = doc.entity('mch:weather', {'prov:label':'NYC Weather Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        turnstile_resource = doc.entity('mta:turnstile', {'prov:label':'Turnstile Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        turnstile_weather_resource = doc.entity('dat:turnstiles_and_weather', {'prov:label':'Turnstile and Weather Data', prov.model.PROV_TYPE:'ont:DataSet'})

        get_weather = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_turnstile = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 
        get_turnstile_weather = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 
        
        doc.wasAssociatedWith(get_weather, this_script)
        doc.wasAssociatedWith(get_turnstile, this_script)
        doc.wasAssociatedWith(get_turnstile_weather, this_script)

        doc.usage(get_weather, weather_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:DataSet'} )
        doc.usage(get_turnstile, turnstile_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:DataSet'} )
        doc.usage(get_turnstile_weather, turnstile_weather_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'} )

        weather = doc.entity('dat:anuragp1_jl101995#weather', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(weather, this_script)
        doc.wasGeneratedBy(weather, get_weather, endTime)
        doc.wasDerivedFrom(weather, weather_resource, get_weather, get_weather, get_weather)

        turnstile = doc.entity('dat:anuragp1_jl101995#turnstile', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(turnstile, this_script)
        doc.wasGeneratedBy(turnstile, get_turnstile, endTime)
        doc.wasDerivedFrom(turnstile, turnstile_resource, get_turnstile, get_turnstile, get_turnstile)

        turnstile_weather = doc.entity('dat:anuragp1_jl101995#turnstiles_and_weather', {prov.model.PROV_LABEL:'', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(turnstile_weather, this_script)
        doc.wasGeneratedBy(turnstile_weather, get_turnstile_weather, endTime)
        doc.wasDerivedFrom(turnstile_weather, turnstile_weather_resource, get_turnstile_weather, get_turnstile_weather, get_turnstile_weather)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()
        
        return doc


transformation3.execute()
doc = transformation3.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
