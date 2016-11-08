'''
CS 591 Project One
projOne.py
jzhou94@bu.edu
katerin@bu.edu
'''
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
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        print("repo: ", repo)
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        '''
        FIREARMS RECOVERY
        '''
        url = 'https://data.cityofboston.gov/resource/ffz3-2uqv.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r0 = json.loads(response)
        s = json.dumps(r0, sort_keys=True, indent=2)
        repo.dropPermanent("firearms")
        repo.createPermanent("firearms")
        repo['jzhou94_katerin.firearms'].insert_many(r0)
        print("firearms loaded")
        
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
            if (this.postal*1 > 2100 && this.postal*1 < 2300 && this.title == "Police Officer")
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
        S = [doc for doc in repo.jzhou94_katerin.schools.find()]
        
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
        C = [doc for doc in repo.jzhou94_katerin.crime.find()]


        """
        MERGE
        """
        repo.dropPermanent("merge")
        repo.createPermanent("merge")
        def product(R, S):
            return [(t, u) for t in R for u in S if(t['_id'] == u['_id'])]
        P = product(S, C)
        j = 0
        for i in P:
            repo['jzhou94_katerin.merge'].insert({'Name': P[j]})
            j = j+1

        print("merge created")
    
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
        repo.authenticate('jzhou94_katerin', 'jzhou94_katerin')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/jzhou94_katerin/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/jzhou94_katerin/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jzhou94_katerin#projOne', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_firearms = doc.entity('bdp:ffz3-2uqv', {'prov:label':'Boston Police Department Firearms Recovery Counts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})       
        resource_employee_earnings = doc.entity('bdp:bejm-5s9g', {'prov:label':'Employee Earnings Report 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_avg_earnings = doc.entity('dat:avg_earnings', {'prov:label':'Average Earnings', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        resource_public_schools = doc.entity('bdp:492y-i77g', {'prov:label':'Boston Public Schools (School Year 2012-2013)', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_schools = doc.entity('dat:schools', {'prov:label':'Number of Schools in Location', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        resource_crime_incident = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Crime Incident Reports (July 2012 - August 2015)', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_crime = doc.entity('dat:crime', {'prov:label':'Crimes per Location', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        resource_merge = doc.entity('dat:merge', {'prov:label':'Crimes to Schools', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})

        get_firearms = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 
        get_employee_earnings = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_avg_earnings = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_public_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_crime_incident = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_merge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_firearms, this_script)
        doc.wasAssociatedWith(get_employee_earnings, this_script)
        doc.wasAssociatedWith(get_avg_earnings, this_script)
        doc.wasAssociatedWith(get_public_schools, this_script)
        doc.wasAssociatedWith(get_schools, this_script)
        doc.wasAssociatedWith(get_crime_incident, this_script)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.wasAssociatedWith(get_merge, this_script)
        
        doc.usage(get_firearms, resource_firearms, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?crimegunsrecovered,gunssurrenderedsafeguarded,gunssurrenderedsafeguarded,collectiondate,buybackgunsrecovered'
                }
            )        
        doc.usage(get_employee_earnings, resource_employee_earnings, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?title,retro,injured,postal,details,other,quinn_education_incentive,regular,department_name,name,total_earnings,overtime'
                }
            )
        doc.usage(get_avg_earnings, resource_avg_earnings, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )
        doc.usage(get_public_schools, resource_public_schools, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?sch_name,location_location,sch_type,location,type,zipcode,location_state,:@computed_region_aywg_kpfh,bldg_name,location_city,location_zip'
                }
            )
        doc.usage(get_schools, resource_schools, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )
        doc.usage(get_crime_incident, resource_crime_incident, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                 'ont:Query':'?compnos,naturecode,x,reptdistrict,reportingarea,location,type,weapontype,:@computed_region_aywg_kpfh,ucrpart,year,main_crimecode,streetname,fromdate,domestic,shift,day_week,shooting,y,month,incident_type_description'
                }
            )
        doc.usage(get_crime, resource_crime, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )
        doc.usage(get_merge, resource_merge, startTime, None,
                {prov.model.PROV_TYPE:'ont:Computation',
                 'ont:Query':'?value'
                }
            )

        firearms = doc.entity('dat:jzhou94_katerin#firearms', {prov.model.PROV_LABEL:'Employee Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(firearms, this_script)
        doc.wasGeneratedBy(firearms, get_firearms, endTime)
        doc.wasDerivedFrom(firearms, resource_firearms, get_firearms, get_firearms, get_firearms)

        employee_earnings = doc.entity('dat:jzhou94_katerin#employee_earnings', {prov.model.PROV_LABEL:'Employee Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(employee_earnings, this_script)
        doc.wasGeneratedBy(employee_earnings, get_employee_earnings, endTime)
        doc.wasDerivedFrom(employee_earnings, resource_employee_earnings, get_employee_earnings, get_employee_earnings, get_employee_earnings)

        avg_earnings = doc.entity('dat:jzhou94_katerin#avg_earnings', {prov.model.PROV_LABEL:'Average Earnings', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(avg_earnings, this_script)
        doc.wasGeneratedBy(avg_earnings, get_avg_earnings, endTime)
        doc.wasDerivedFrom(avg_earnings, resource_avg_earnings, get_avg_earnings, get_avg_earnings, get_avg_earnings)
        
        public_schools = doc.entity('dat:jzhou94_katerin#public_schools', {prov.model.PROV_LABEL:'Public Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(public_schools, this_script)
        doc.wasGeneratedBy(public_schools, public_schools, endTime)
        doc.wasDerivedFrom(public_schools, resource_public_schools, public_schools, get_public_schools, get_public_schools)

        schools = doc.entity('dat:jzhou94_katerin#schools', {prov.model.PROV_LABEL:'Number of Schools in Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource_schools, get_schools, get_schools, get_schools)

        crime_incident = doc.entity('dat:jzhou94_katerin#crime_incident', {prov.model.PROV_LABEL:'Crime Incident', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_incident, this_script)
        doc.wasGeneratedBy(crime_incident, get_crime_incident, endTime)
        doc.wasDerivedFrom(crime_incident, resource_crime_incident, get_crime_incident, get_crime_incident, get_crime_incident)

        crime = doc.entity('dat:jzhou94_katerin#crime', {prov.model.PROV_LABEL:'Crimes per Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, resource_crime, get_crime, get_crime, get_crime)

        merge = doc.entity('dat:jzhou94_katerin#merge', {prov.model.PROV_LABEL:'Crimes to Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(merge, this_script)
        doc.wasGeneratedBy(merge, get_merge, endTime)
        doc.wasDerivedFrom(merge, resource_merge, get_merge, get_merge, get_merge)
        
        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
