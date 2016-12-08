import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
#from pprint import pprint
import math
import matplotlib.pyplot as plt
from pylab import *

class crime_time_correlation(dml.Algorithm):
    contributor = 'cyung20_kwleung'
    reads = ['cyung20_kwleung.crime']
    writes = ['cyung20_kwleung.time_cor']

    @staticmethod
    def execute(trial = True):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cyung20_kwleung', 'cyung20_kwleung')


        # List of hours (in military time)
        data = repo['cyung20_kwleung.crime'].find()
        hours_set = set()
        
        # Iterates through crimes and adds the hour each crime was committed to hours_set.
        # Also creates a new column in the crime reports csv with said hours according to
        # the crime they correspond with
        for d in data:
            try:
                hours_set.add(int(dict(d)['occurred_on_date'][11:13]))
                repo['cyung20_kwleung.crime'].update_many({'_id': d['_id']},
                {'$set': {'hour': (int(dict(d)['occurred_on_date'][11:13]))},})
            except KeyError:
                dict(d)['occurred_on_date'] = None

#        Prints out the updated dataset
#        for document in repo['cyung20_kwleung.crime'].find():
#            pprint(document)

        hours_set = list(hours_set)
        crime_counts = [0] * len(hours_set)

        data = repo['cyung20_kwleung.crime'].find()
        
        # Iterates through the crimes and counts the number of crimes that occur at each hour of the day
        index = 0
        for d in data:
            if (index == 24):
                break
            
            hour = hours_set[index]
            num_crimes_at_hour = repo['cyung20_kwleung.crime'].count({'hour': hour})
            crime_counts[index] = num_crimes_at_hour
#            print(hour, num_crimes_at_hour)
            index += 1
        
        
        # The following code takes the vectors 'hours_set' and 'crime_counts' and calculates their correlation coefficient
        num_hours = len(hours_set)
        hours_mean = 0
        num_crimes_mean = 0
        
        for x in range(num_hours):
            hours_mean += hours_set[x]
            num_crimes_mean += crime_counts[x]
        
        hours_mean = hours_mean / num_hours
        num_crimes_mean = num_crimes_mean / num_hours
        
        temp_a = [0] * num_hours
        temp_b = [0] * num_hours
        for x in range(num_hours):
            temp_a[x] = hours_set[x] - hours_mean
            temp_b[x] = crime_counts[x] - num_crimes_mean
            
        prod_ab = 0
        for x in range(num_hours):
            prod_ab += temp_a[x] * temp_b[x]

        sum_a = 0
        sum_b = 0
        for x in range(num_hours):
            sum_a += temp_a[x] ** 2
            sum_b += temp_b[x] ** 2

        # Calculate and print the correlation coefficient
        correlation_coefficient = prod_ab / math.sqrt(sum_a * sum_b)
        print("correlation_coefficient = ", correlation_coefficient)
        
        
        # Graphs the correlation
        plt.title("Correlation Between Crimes and Hour They Were Committed")
        plt.xlabel("Hours of the Day (in military time)")
        plt.ylabel("Number of Crimes Reported")
        plt.axis([-1, 25, 0, 9000])
        
        # m = slope, b = y-intercept for the linear regression line
        (m,b) = polyfit(hours_set, crime_counts, 1)
        yp = polyval([m,b], hours_set)
        plt.plot(hours_set, yp, color="red")
        plt.scatter(hours_set, crime_counts)
        plt.show()
        
        repo.logout()
        
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

          # Set up the database connection.
         client = dml.pymongo.MongoClient()
         repo = client.repo
         repo.authenticate('cyung20_kwleung', 'cyung20_kwleung')

         doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
         doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
         doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
         doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
         doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

         this_script = doc.agent('alg:cyung20_kwleung#crime_time_correlation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

         crime_resource = doc.entity('dat:cyung20_kwleung#crime', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet'})

         this_merge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Correlation between crimes and time they were committed', prov.model.PROV_TYPE:'ont:Computation'})

         doc.wasAssociatedWith(this_merge, this_script)
         doc.usage(this_merge, crime_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

         crime_time = doc.entity('dat:cyung20_kwleung#crime_time_correlation', {prov.model.PROV_LABEL:'Correlation between crimes and time they were committed', prov.model.PROV_TYPE:'ont:DataSet'})
         doc.wasAttributedTo(crime_time, this_script)
         doc.wasGeneratedBy(crime_time, this_merge, endTime)
         doc.wasDerivedFrom(crime_time, crime_resource, this_merge, this_merge, this_merge)

         repo.record(doc.serialize()) # Record the provenance document.
         repo.logout()

         return doc


crime_time_correlation.execute()
doc = crime_time_correlation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof