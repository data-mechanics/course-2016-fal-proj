import dml
import pymongo
import json
import uuid
import prov.model
import datetime
import requests

def project(R, p):
  return [p(t) for t in R]

def geoCode(doc):
      def url_format(street, city):
        return 'https://geocoding.geo.census.gov/geocoder/locations/address?street={}&city={}&state=MA&benchmark=Public_AR_Census2010&vintage=Census2010_Census&format=json'.format(street, city)
      street = doc['location'].replace(' ', '+') 
      city = doc['city']
      city = 'Boston' if city == 'OTHER' or city == 'NO DATA ENTERED' else city
      response = requests.get(url_format(street, city))
      if response.status_code == 200:
        try:
          data = response.json()
          x = data['result']['addressMatches'][0]['coordinates']['x']
          y = data['result']['addressMatches'][0]['coordinates']['y']
          doc['geometry'] = {
              'type': 'Point',
              'coordinates': [y, x]
          }
        except:
         doc['geometry'] = None 

      return doc

class DataFetchAndStore(dml.Algorithm):
  contributor = 'mgerakis'
  reads = []
  writes = ['mgerakis.fio', 'mgerakis.crime', 'mgerakis.salary_2014', 'mgerakis.police_stations']

  @staticmethod
  def fetchAndStore(repo, url, collection):
    response = requests.get(url)
    if response.status_code == 200:
      data = response.json()
      if collection == 'mgerakis.fio':
        fios = [doc for doc in data]
        project(fios, geoCode)
        data = fios
      repo.dropPermanent(collection)
      repo.createPermanent(collection)
      repo[collection].insert_many(data)
      if collection == 'mgerakis.fio':
        repo[collection].create_index(['geometry', pymongo.GEO2D])

  @staticmethod
  def setupDB():
    # Set up the database connection.
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('mgerakis', 'mgerakis')
    return repo

  @staticmethod
  def execute(trial = False):
    ''' Retrieve data sets '''
    startTime = datetime.datetime.now()
    
    repo = DataFetchAndStore.setupDB()
    
    datasets = {
      'mgerakis.fio': 'https://data.cityofboston.gov/resource/2pem-965w.json',
      'mgerakis.crime': 'https://data.cityofboston.gov/resource/ufcx-3fdn.json',
      'mgerakis.salary_2014': 'https://data.cityofboston.gov/resource/ntv7-hwjm.json',
      'mgerakis.police_stations': 'https://data.cityofboston.gov/resource/pyxn-r3i2.json'
    }

    for collection, url in datasets.items():
      DataFetchAndStore.fetchAndStore(repo, url, collection)

    repo.logout()

    endTime = datetime.datetime.now()

    return {'start': startTime, 'end': endTime}

  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    ''' Create provenance document describing everything happening in this script '''

    # Set up the database connection.
    repo = DataFetchAndStore.setupDB()

    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
    doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

    this_script = doc.agent('alg:mgerakis#data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

    fio_resource = doc.entity('bdp:2pem-965w', {'prov:label':'Boston Police Department FIO', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    crime_resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Crime Incident Reports (July 2012 - August 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    salary_resource = doc.entity('bdp:ntv7-hwjm', {'prov:label':'Employee Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    stations_resource = doc.entity('bdp:pyxn-r3i2', {'prov:label':'Boston Police District Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})


    get_fio = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
    get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
    get_salary = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
    get_stations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)


    doc.wasAssociatedWith(get_fio, this_script)
    doc.wasAssociatedWith(get_crime, this_script)
    doc.wasAssociatedWith(get_salary, this_script)
    doc.wasAssociatedWith(get_stations, this_script)

    doc.usage(get_fio, fio_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    doc.usage(get_crime, crime_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    doc.usage(get_salary, salary_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    doc.usage(get_stations, stations_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    fio = doc.entity('dat:mgerakis#fio', {prov.model.PROV_LABEL:'Boston Police Department FIO', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(fio, this_script)
    doc.wasGeneratedBy(fio, get_fio, endTime)
    doc.wasDerivedFrom(fio, fio_resource, get_fio, get_fio, get_fio)

    crime = doc.entity('dat:mgerakis#crime', {prov.model.PROV_LABEL:'Crime Incident Reports (July 2012 - August 2015', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(crime, this_script)
    doc.wasGeneratedBy(crime, get_crime, endTime)
    doc.wasDerivedFrom(crime, crime_resource, get_crime, get_crime, get_crime)

    salary = doc.entity('dat:mgerakis#salary', {prov.model.PROV_LABEL:'Employee Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(salary, this_script)
    doc.wasGeneratedBy(salary, get_salary, endTime)
    doc.wasDerivedFrom(salary, salary_resource, get_salary, get_salary, get_salary)

    stations = doc.entity('dat:mgerakis#stations', {prov.model.PROV_LABEL:'Boston Police District Stations', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(stations, this_script)
    doc.wasGeneratedBy(stations, get_stations, endTime)
    doc.wasDerivedFrom(stations, stations_resource, get_stations, get_stations, get_stations)

    repo.record(doc.serialize())
    repo.logout()

    return doc


DataFetchAndStore.execute()
doc = DataFetchAndStore.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
