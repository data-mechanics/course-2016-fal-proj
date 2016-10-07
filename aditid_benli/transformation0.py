import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from geopy.distance import vincenty


class transformation0(dml.Algorithm):
    contributor = 'aditid_benli'
    reads = ['aditid_benli.comparking', 'aditid_benli.partickets']
    writes = ['aditid_benli.ComParkTick']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('aditid_benli', 'aditid_benli')
        
        comParkRepo = repo.aditid_benli.comparking
        ticketsRepo = repo.aditid_benli.partickets


        lots = comParkRepo.find()
        Lots = []
        for l in lots:
            l['nearbyIllegalParking'] = []
            Lots.append(l)
         
        tickets = ticketsRepo.find()


        relavent_tickets = []
        #relavent_tickets.append(tickets[5])
        for t in tickets:
            # if len(relavent_tickets) > 1:
            #     break
            if 'location' in t and (t['violation_description'] == "RESIDENT PERMIT ONLY" or t['violation_description'] == " NO PARKING" or t['violation_description'] == " NO STOPPING"):
                relavent_tickets.append(t)
        print (len(relavent_tickets))
        # for r in relavent_tickets:
        #     print (r)
        

        #print (relavent_tickets[0])
        for rt in relavent_tickets:
            #print (rt)
            #print (rt['location'])
            closestDistance = vincenty(rt['location']['coordinates'],Lots[0]['the_geom']['coordinates']).miles
            closestLot = 0
            for i in range(len(Lots)):
                candidateDist = vincenty(rt['location']['coordinates'],Lots[i]['the_geom']['coordinates']).miles
                # print (type(candidateDist))
                # print (type(closestDistance))  
                if (candidateDist < closestDistance):
                    #print (i)
                    closestDistance = candidateDist
                    closestLot = i
            #print ("sha")
            # print (closestLot)
            #print (rt)

            #print ('gra')
            #print closestLot
            Lots[closestLot]['nearbyIllegalParking'].append(rt)

            #print (Lots[closestLot])


        print ('Violations per Area')
        for l in Lots:
            print (len(l['nearbyIllegalParking']))

        # print (Lots[13]['nearbyIllegalParking'])
        print ('my only regret bonitis')

        repo.dropPermanent("LotsWithAdjacentTickets")
        repo.createPermanent("LotsWithAdjacentTickets")
        repo['aditid_benli.LotsWithAdjacentTickets'].insert_many(Lots)


        
        #repo.aditid_benli.jam.map_reduce(map_function, reduce_function, 'aditid_benli.ComParkTick');
        
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
        repo.authenticate('aditid_benli', 'aditid_benli')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aditid_benli#transformation0', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_script = doc.agent('alg:aditid_benli#transformation0', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        ComParkTick = doc.entity('dat:aditid_benli#ComParkTick', {prov.model.PROV_LABEL:'Commercial Parking Ticketing', prov.model.PROV_TYPE:'ont:DataSet'})        
        getComParkTick = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Vincenty distance to cluster tickets to parking'})        
        doc.wasAssociatedWith(getComParkTick, this_script)
        doc.used(getComParkTick, ComParkTick, startTime)
        doc.wasAttributedTo(ComParkTick, this_script)
        doc.wasGeneratedBy(ComParkTick, getComParkTick, endTime)   


        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

transformation0.execute()
doc = transformation0.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof