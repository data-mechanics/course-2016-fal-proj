import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class example(dml.Algorithm):
    contributor = 'jzhou94_katerin'
    reads = []
    writes = ['jzhou94_katerin.employee_earnings', 'jzhou94_katerin.public_schools', 'jzhou94_katerin.crime_incident', 'jzhou94_katerin.police_station', 'jzhou94_katerin.education']

    @staticmethod
    def execute(trial = False):
        print("starting data retrieval")
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')
        
        ''' EMPLOYEE EARNINGS '''
        url = 'https://data.cityofboston.gov/resource/bejm-5s9g.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("employee_earnings")
        repo.createPermanent("employee_earnings")
        repo['jzhou94_katerin.employee_earnings'].insert_many(r)
        print("employee earnings loaded")

        map_function_avg_earnings = Code('''function() {
            if (this.postal*1 > 2100 && this.postal*1 < 2300)
            emit(this.postal, {tot:this.total_earnings, n: 1, avg: this.total_earnings});
            }''')
        
        reduce_function_avg_earnings = Code('''function(k, vs) {            
            var total = 0;
            var counts = 0;
            for (var i = 0; i < vs.length; i++)
            total += (vs[i].tot*1);
            for (var i = 0; i < vs.length; i++)
            counts += vs[i].n;
            
            return {tot:total.toFixed(2), n: counts, avg: (total/counts).toFixed(2)};
            }''')
        
        repo.dropPermanent('jzhou94_katerin.avg_earnings')
        repo.createPermanent('jzhou94_katerin.avg_earnings')
        repo.jzhou94_katerin.employee_earnings.map_reduce(map_function_avg_earnings, reduce_function_avg_earnings, 'jzhou94_katerin.avg_earnings');
        
        print("average earnings data created")
        
        ''' PUBLIC SCHOOLS '''
        url = 'https://data.cityofboston.gov/resource/492y-i77g.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("public_schools")
        repo.createPermanent("public_schools")
        repo['jzhou94_katerin.public_schools'].insert_many(r)
        print("public schools loaded")

        map_function_school = Code('''function() {
            emit('0'+this.zipcode, {schools:1});
            }''')
        
        reduce_function_school = Code('''function(k, vs) {
            var total = 0;
            for (var i = 0; i < vs.length; i++)
                total += vs[i].schools;
            return {schools:total};
            }''')
        
        repo.dropPermanent('jzhou94_katerin.schools')
        repo.createPermanent('jzhou94_katerin.schools')
        repo.jzhou94_katerin.public_schools.map_reduce(map_function_school, reduce_function_school, 'jzhou94_katerin.schools');

        print("school data created")
        
        ''' CRIME INCIDENTS '''
        url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropPermanent("crime_incident")
        repo.createPermanent("crime_incident")
        repo['jzhou94_katerin.crime_incident'].insert_many(r)
        print("crime incidents loaded")

        map_function_crime = Code('''
            function() {
            district = this.reptdistrict
            if(district == 'A1' || district == 'A15')
                emit('02120', {crime:1});
            else if(district == 'A7')
                emit('02128', {crime:1});
            else if(district == 'B2')
                emit('02119', {crime:1});
            else if(district == 'B3')
                emit('02124', {crime:1});
            else if(district == 'C6')
                emit('02127', {crime:1});
            else if(district == 'C11')
                emit('02122', {crime:1});
            else if(district == 'D4')
                emit('02116', {crime:1});
            else if(district == 'D14')
                emit('02135', {crime:1});
            else if(district == 'E5')
                emit('02132', {crime:1});
            else if(district == 'E13')
                emit('02130', {crime:1});
            else if(district == 'E18')
                emit('02136', {crime:1});
            }''')
        
        reduce_function_crime = Code('''function(k, vs) {
            var total = 0;
            for (var i = 0; i < vs.length; i++)
               total += vs[i].crime;
            return {crime:total};
            }''')

        repo.dropPermanent('jzhou94_katerin.crime')
        repo.createPermanent('jzhou94_katerin.crime')
        repo.jzhou94_katerin.crime_incident.map_reduce(map_function_crime, reduce_function_crime, 'jzhou94_katerin.crime');
        print("crime created")
        
        
        repo.logout()

        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        return 0


example.execute()
