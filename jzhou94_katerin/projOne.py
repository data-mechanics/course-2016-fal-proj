import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = []
    writes = ['jzhou94_katerin.employee_earnings', 'jzhou94_katerin.public_schools', 'jzhou94_katerin.crime_incident', 'jzhou94_katerin.police_station', 'jzhou94_katerin.education']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        ''' EMPLOYEE EARNINGS '''
        url = 'https://data.cityofboston.gov/resource/bejm-5s9g.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        # s print everything in the databse
        repo.dropPermanent("employee_earnings")
        repo.createPermanent("employee_earnings")
        repo['jzhou94_katerin.employee_earnings'].insert_many(r)

        ''' PUBLIC SCHOOLS '''
        url = 'https://data.cityofboston.gov/resource/492y-i77g.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        # s print everything in the databse
        repo.dropPermanent("public_schools")
        repo.createPermanent("public_schools")
        repo['jzhou94_katerin.public_schools'].insert_many(r)

        ''' CRIME INCIDENTS '''
        url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        # s print everything in the databse
        repo.dropPermanent("crime_incident")
        repo.createPermanent("crime_incident")
        repo['jzhou94_katerin.crime_incident'].insert_many(r)

        ''' POLICE STATIONS '''
        url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        # s print everything in the databse
        repo.dropPermanent("police_station")
        repo.createPermanent("police_station")
        repo['jzhou94_katerin.police_station'].insert_many(r)

        ''' EDUCATION '''
        url = 'https://odn.data.socrata.com/resource/7mfb-7yvj.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        # s print everything in the databse
        repo.dropPermanent("education")
        repo.createPermanent("education")
        repo['jzhou94_katerin.education'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return 0


example.execute()
