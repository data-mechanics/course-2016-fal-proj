import dml
import prov.model
import datetime
import requests

def project(R, p):
  return [p(t) for t in R]

def format_name(name):
  tmp = project(name.split(' '), str.title)
  tmp.reverse()
  return ', '.join(tmp)

class FioOvertime(dml.Algorithm):
  contributor = 'mgerakis'
  reads = ['mgerakis.fio', 'mgerakis.salary']
  writes = ['mgerakis.fios_overtime']

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

    repo = FioOvertime.setupDB()

    fios = [doc for doc in repo['mgerakis.fio'].find()]

    officer_fio = {}

    for fio in fios:
      name = format_name(fio['officer'])
      if name in officer_fio:
        officer_fio[name].append(fio)
      else:
        officer_fio[name] = [fio]

    fios_overtime = []
    for officer in officer_fio:
      white = 0
      other = 0
      fios = officer_fio[officer]
      for fio in fios:
        if fio['race_desc'] == 'B(Black)' or fio['race_desc'] == 'H(Hispanic)':
          other += 1
        if fio['race_desc'] == 'W(White)':
          white += 1
      salary_record = [doc for doc in repo['mgerakis.salary_2014'].find({'name': officer})]
      fios_overtime.append({
        'officer': officer,
        'counts': {'white': white, 'other': other},
        'salary': salary_record,
        'data': officer_fio[officer]
      })


    repo.dropPermanent('fios_overtime')
    repo.createPermanent('fios_overtime')
    repo['fios_overtime'].insert_many(fios_overtime)

    endTime = datetime.datetime.now()

    return { 'startTime': startTime, 'endTime': endTime }

  @staticmethod
  def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
    repo = FioOvertime.setupDB()

    doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
    doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
    doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
    doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
    doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

    this_script = doc.agent('alg:mgerakis#fios_overtime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
    fio_db = doc.entity('dat:mgerakis#fio', {'prov:label':'Boston Police Department FIO', prov.model.PROV_TYPE:'ont:DataSet'})
    salary_db = doc.entity('dat:mgerakis#salary', {'prov:label':'Boston Police Department FIO', prov.model.PROV_TYPE:'ont:DataSet'})

    transform_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
    doc.wasAssociatedWith(transform_data, this_script)

    doc.usage(transform_data, fio_db, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
    doc.usage(transform_data, salary_db, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

    fios_overtime = doc.entity('dat:mgerakis#fios_overtime', {prov.model.PROV_LABEL:'Boston Police Officers Overtime vs FIOs', prov.model.PROV_TYPE:'ont:DataSet'})
    doc.wasAttributedTo(fios_overtime, this_script)
    doc.wasGeneratedBy(fios_overtime, transform_data, endTime)
    doc.wasDerivedFrom(fios_overtime, fio_db, transform_data, transform_data, transform_data)
    doc.wasDerivedFrom(fios_overtime, salary_db, transform_data, transform_data, transform_data)

    repo.record(doc.serialize())
    repo.logout()


FioOvertime.execute()
