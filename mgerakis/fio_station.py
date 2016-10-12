import dml
import prov.model
import datetime
import requests

def project(R, p):
  return [p(t) for t in R]

class FioStation(dml.Algorithm):
  contributor = 'mgerakis'
  reads = ['mgerakis.fio', 'mgerakis.station']
  writes = ['mgerakis.fios_station']

  @staticmethod
  def setupDB():
    # Set up the database connection.
    client = dml.pymongo.MongoClient()
    repo = client.repo
    repo.authenticate('mgerakis', 'mgerakis')
    return repo

  @staticmethod
  def execute(trial = False):
    startTime = datetime.datetime.now()

    repo = FioStation.setupDB()

    stations = [doc for doc in repo['mgerakis.stations'].find()]

    fios_station = []

    for station in stations:
      fios = repo['mgerakis.fio'].find({
        'geometry': {
          '$near': [station['CENTROIDY'], station['CENTROIDX']] 
        }})
      fios_station.push({'location': station['NAME'], 'fios': fios})
      
    for station in fios_station:
      white = 0
      other = 0
      for fio in station['fios']:
        if fio['RACE_DESC'] == 'B(Black)' or fio['RACE_DESC'] == 'H(Hispanic)':
          other += 1
        if fio['RACE_DESC'] == 'W(White)':
          white += 1
      station['counts'] = {
          'white': white,
          'other': other
      }

    repo.dropPermanent('mgerakis.fios_station')
    repo.createPermanent('mgerakis.fios_station')
    repo['mgerakis.fios_station'].insert_many(fios_station)

    endTime = datetime.datetime.now()

    return { 'startTime': startTime, 'endTime': endTime }

  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    repo = FioStation.setupDB()

    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
    doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

    this_script = doc.agent('alg:mgerakis#fio_station', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
    fio_db = doc.entity('dat:mgerakis#fio', {'prov:label':'Boston Police Department FIO', prov.model.PROV_TYPE:'ont:DataSet'})
    station_db = doc.entity('dat:mgerakis#station', {'prov:label':'Boston Police Department FIO', prov.model.PROV_TYPE:'ont:DataSet'})

    transform_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(transform_data, this_script)

    doc.usage(transform_data, fio_db, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    doc.usage(transform_data, station_db, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    fios_station = doc.entity('dat:mgerakis#fios_station', {prov.model.PROV_LABEL:'Boston FIOs per Police Stations', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(fios_station, this_script)
    doc.wasGeneratedBy(fios_station, transform_data, endTime)
    doc.wasDerivedFrom(fios_station, fio_db, transform_data, transform_data, transform_data)
    doc.wasDerivedFrom(fios_station, station_db, transform_data, transform_data, transform_data)

    repo.record(doc.serialize())
    repo.logout()

FioStation.execute()
