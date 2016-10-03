import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import googlemaps
import re

class example(dml.Algorithm):
    contributor = 'ktan_ngurung'
    reads = []
    writes = ['ktan_ngurung.bigBelly', 'ktan_ngurung.colleges', 'ktan_ngurung.hubways', 'ktan_ngurung.busStops', 'ktan_ngurung.tStops']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')

        repo.dropPermanent("tRidershipLocation")
        repo.createPermanent("tRidershipLocation")

        # Get ridership and tstop location data
        ridership = repo.ktan_ngurung.ridership.find()
        tstops = repo.ktan_ngurung.tStops.find()
        collection = {}

        gmaps = googlemaps.Client(key='AIzaSyCnbPRMgv_MDYaPqiq2mVYIpWUy-m_k3Jc')

        # Commented block below can only be a when 2500 quota limit for Google Maps API has not been used up
        '''
        for doc in tstops:
            docDict1 = dict(doc)
            for station in docDict1['stations']:

                lat = station['latitude']
                lon = station['longitude']

                reverse_geocode_result = gmaps.reverse_geocode((lat, lon))
                address = reverse_geocode_result[0]['formatted_address']
                reg = re.compile('^.*(?P<zipcode>\d{5}).*$')
                match = reg.match(address)

                # Build collection dictionary of the format {station name: {zipcode, entries}, etc.}
                if match:
                    zipcode = match.groupdict()['zipcode']
                    collection[station['title']] = {'zipcode': zipcode, 'entries': 0}
        '''

        # Hard coded dictionary built from above code in variable collection due to API quota limit
        collection = {'Wellesley Hills': {'zipcode': '02481', 'entries': 0}, 'Needham Center': {'zipcode': '02492', 'entries': 0}, 'Woodland Station': {'zipcode': '02466', 'entries': 0}, 'Central Avenue Station': {'zipcode': '02186', 'entries': 0}, 'Kenmore Station': {'zipcode': '02215', 'entries': 0}, 'Winchester Center': {'zipcode': '01890', 'entries': 0}, 'Fenwood Road Station': {'zipcode': '02115', 'entries': 0}, 'Rowley': {'zipcode': '01969', 'entries': 0}, 'Lynn': {'zipcode': '01901', 'entries': 0}, 'Fields Corner Station': {'zipcode': '02122', 'entries': 0}, 'Northern Avenue at Tide Street': {'zipcode': '02210', 'entries': 0}, 'Swampscott': {'zipcode': '01907', 'entries': 0}, 'Boston College Station': {'zipcode': '02467', 'entries': 0}, 'Fairmount': {'zipcode': '02136', 'entries': 0}, 'East Weymouth Station': {'zipcode': '02189', 'entries': 0}, 'Abington': {'zipcode': '02351', 'entries': 0}, 'Davis Station': {'zipcode': '02144', 'entries': 0}, 'Needham Heights': {'zipcode': '02494', 'entries': 0}, 'Valley Road Station': {'zipcode': '02186', 'entries': 0}, 'Brookline Hills Station': {'zipcode': '02445', 'entries': 0}, 'West Gloucester': {'zipcode': '01930', 'entries': 0}, 'Maverick Station': {'zipcode': '02128', 'entries': 0}, 'Griggs Street / Long Avenue Station': {'zipcode': '02134', 'entries': 0}, 'Lincoln': {'zipcode': '01773', 'entries': 0}, 'Sullivan Square Station (Cambridge Street Exit)': {'zipcode': '02129', 'entries': 0}, 'Green Street Station': {'zipcode': '02130', 'entries': 0}, 'St. Marys Street Station': {'zipcode': '02446', 'entries': 0}, 'Sharon': {'zipcode': '02067', 'entries': 0}, 'Montserrat': {'zipcode': '01915', 'entries': 0}, 'Shawmut Station': {'zipcode': '02124', 'entries': 0}, 'Islington': {'zipcode': '02090', 'entries': 0}, 'Malden Center': {'zipcode': '02148', 'entries': 0}, 'Melrose/Cedar Park': {'zipcode': '02176', 'entries': 0}, 'Ruggles Station': {'zipcode': '02120', 'entries': 0}, 'Aquarium Station': {'zipcode': '02109', 'entries': 0}, 'Kendal Green': {'zipcode': '02493', 'entries': 0}, 'St. Paul Street Station (B)': {'zipcode': '02446', 'entries': 0}, 'Alewife Station': {'zipcode': '02140', 'entries': 0}, 'Chestnut Hill Station': {'zipcode': '02467', 'entries': 0}, 'Riverside Station': {'zipcode': '02466', 'entries': 0}, '306 Northern Avenue': {'zipcode': '02127', 'entries': 0}, 'Dean Road Station': {'zipcode': '02445', 'entries': 0}, 'Boston University East Station': {'zipcode': '02215', 'entries': 0}, 'Prides Crossing': {'zipcode': '01915', 'entries': 0}, '21 Dry Dock Avenue': {'zipcode': '02210', 'entries': 0}, 'Yawkey': {'zipcode': '02215', 'entries': 0}, 'Andover': {'zipcode': '01810', 'entries': 0}, 'Plymouth': {'zipcode': '02360', 'entries': 0}, 'Capen Street Station': {'zipcode': '02186', 'entries': 0}, 'Coolidge Corner Station': {'zipcode': '02446', 'entries': 0}, 'Framingham': {'zipcode': '01702', 'entries': 0}, 'Ashland': {'zipcode': '01721', 'entries': 0}, 'South Weymouth': {'zipcode': '02190', 'entries': 0}, 'Wedgemere': {'zipcode': '01890', 'entries': 0}, 'Pleasant Street Station': {'zipcode': '02446', 'entries': 0}, 'Reservoir Station': {'zipcode': '02135', 'entries': 0}, 'Packards Corner Station': {'zipcode': '02215', 'entries': 0}, 'Haymarket Station': {'zipcode': '02114', 'entries': 0}, 'Roxbury Crossing Station': {'zipcode': '02120', 'entries': 0}, 'Kent Street Station': {'zipcode': '02446', 'entries': 0}, 'Holbrook/Randolph': {'zipcode': '02368', 'entries': 0}, 'Bellevue': {'zipcode': '02132', 'entries': 0}, 'Cleveland Circle Station': {'zipcode': '02135', 'entries': 0}, 'Arlington Station': {'zipcode': '02116', 'entries': 0}, 'Brookline Village Station': {'zipcode': '02445', 'entries': 0}, 'West Hingham Station': {'zipcode': '02043', 'entries': 0}, 'Waban Station': {'zipcode': '02468', 'entries': 0}, 'Revere Beach Station': {'zipcode': '02151', 'entries': 0}, '25 Dry Dock Avenue': {'zipcode': '02210', 'entries': 0}, 'Shirley': {'zipcode': '01464', 'entries': 0}, 'State Station': {'zipcode': '02109', 'entries': 0}, 'Chestnut Hill Avenue Station': {'zipcode': '02135', 'entries': 0}, 'Hynes Convention Center/ICA Station': {'zipcode': '02115', 'entries': 0}, 'Sullivan Square Station (Broadway Exit)': {'zipcode': '02129', 'entries': 0}, 'Butler Station': {'zipcode': '02124', 'entries': 0}, 'Science Park Station': {'zipcode': '02114', 'entries': 0}, 'Whitman': {'zipcode': '02382', 'entries': 0}, 'North Billerica': {'zipcode': '01862', 'entries': 0}, 'Norwood Central': {'zipcode': '02062', 'entries': 0}, 'Ipswich': {'zipcode': '01938', 'entries': 0}, 'Hawes Street Station': {'zipcode': '02446', 'entries': 0}, 'Wood Island Station': {'zipcode': '02128', 'entries': 0}, 'Longwood Station': {'zipcode': '02446', 'entries': 0}, 'Melnea Cass Boulevard Station': {'zipcode': '02119', 'entries': 0}, 'Mission Park Station': {'zipcode': '02115', 'entries': 0}, 'Worcester / Union': {'zipcode': '01604', 'entries': 0}, 'Forest Hills Station': {'zipcode': '02130', 'entries': 0}, 'Boston University Central Station': {'zipcode': '02215', 'entries': 0}, 'Morton Street': {'zipcode': '02126', 'entries': 0}, 'Union Park Street Station': {'zipcode': '02118', 'entries': 0}, 'Walpole': {'zipcode': '02081', 'entries': 0}, 'Dedham Corporate': {'zipcode': '02026', 'entries': 0}, 'Natick': {'zipcode': '01760', 'entries': 0}, 'North Beverly': {'zipcode': '01915', 'entries': 0}, 'Halifax': {'zipcode': '02338', 'entries': 0}, 'Allston Street Station': {'zipcode': '02135', 'entries': 0}, 'Porter Square Station': {'zipcode': '02140', 'entries': 0}, 'Dudley Square Station': {'zipcode': '02119', 'entries': 0}, 'Mansfield': {'zipcode': '02048', 'entries': 0}, 'Newton Highland Station': {'zipcode': '02461', 'entries': 0}, 'Norwood Depot': {'zipcode': '02062', 'entries': 0}, 'Bridgewater': {'zipcode': '02324', 'entries': 0}, 'Tufts (formerly New England) Medical Center Station': {'zipcode': '02111', 'entries': 0}, 'Oak Grove Station': {'zipcode': '02148', 'entries': 0}, 'Northern Avenue at Harbor Street': {'zipcode': '02210', 'entries': 0}, 'Terminal A': {'zipcode': '02128', 'entries': 0}, 'Silver Hill': {'zipcode': '02493', 'entries': 0}, 'Needham Junction': {'zipcode': '02492', 'entries': 0}, 'Englewood Avenue Station': {'zipcode': '02445', 'entries': 0}, 'North Station': {'zipcode': '02114', 'entries': 0}, 'Wellington Station': {'zipcode': '02155', 'entries': 0}, 'Westborough': {'zipcode': '01581', 'entries': 0}, 'Highland': {'zipcode': '02132', 'entries': 0}, 'Providence': {'zipcode': '02903', 'entries': 0}, 'Back of the Hill Station': {'zipcode': '02130', 'entries': 0}, 'Windsor Gardens': {'zipcode': '02062', 'entries': 0}, 'Courthouse Station': {'zipcode': '02210', 'entries': 0}, 'Longwood Medical Area Station': {'zipcode': '02115', 'entries': 0}, 'Northeastern University Station': {'zipcode': '02115', 'entries': 0}, 'Fitchburg': {'zipcode': '01420', 'entries': 0}, 'West Roxbury': {'zipcode': '02132', 'entries': 0}, 'Ballardvale': {'zipcode': '01810', 'entries': 0}, 'West Concord': {'zipcode': '01742', 'entries': 0}, 'Wakefield': {'zipcode': '01880', 'entries': 0}, 'Southborough': {'zipcode': '01772', 'entries': 0}, 'Ashmont Station': {'zipcode': '02124', 'entries': 0}, 'Design Center': {'zipcode': '02210', 'entries': 0}, '(Logan) Airport Station': {'zipcode': '02128', 'entries': 0}, 'World Trade Center Station': {'zipcode': '02210', 'entries': 0}, 'Newburyport': {'zipcode': '01950', 'entries': 0}, 'Boston University West Station': {'zipcode': '02446', 'entries': 0}, 'Hersey': {'zipcode': '02492', 'entries': 0}, 'Stoughton': {'zipcode': '02072', 'entries': 0}, 'Chelsea': {'zipcode': '02150', 'entries': 0}, 'Tappan Street Station': {'zipcode': '02445', 'entries': 0}, 'Wellesley Farms': {'zipcode': '02481', 'entries': 0}, 'Suffolk Downs Station': {'zipcode': '02128', 'entries': 0}, 'Washington Square Station': {'zipcode': '02446', 'entries': 0}, 'Beaconsfield Station': {'zipcode': '02445', 'entries': 0}, 'Wonderland Station': {'zipcode': '02151', 'entries': 0}, 'Greenbush Station': {'zipcode': '02066', 'entries': 0}, 'Charles/Massachusetts General Hospital Station': {'zipcode': '02114', 'entries': 0}, 'Back Bay Station': {'zipcode': '02116', 'entries': 0}, 'Hanson': {'zipcode': '02341', 'entries': 0}, 'South Station': {'zipcode': '02111', 'entries': 0}, 'Harvard Avenue Station': {'zipcode': '02134', 'entries': 0}, 'Harvard Square Station': {'zipcode': '02138', 'entries': 0}, 'South Acton': {'zipcode': '01720', 'entries': 0}, 'Heath Street Station': {'zipcode': '02130', 'entries': 0}, 'Chinatown Station': {'zipcode': '02116', 'entries': 0}, 'West Medford': {'zipcode': '02155', 'entries': 0}, 'Babcock Street Station': {'zipcode': '02215', 'entries': 0}, 'Brandeis/Roberts': {'zipcode': '02453', 'entries': 0}, 'Museum of Fine Arts Station': {'zipcode': '02115', 'entries': 0}, 'Bowdoin Station': {'zipcode': '02114', 'entries': 0}, 'Washington Street Station': {'zipcode': '02135', 'entries': 0}, 'Littleton/Route 495': {'zipcode': '01460', 'entries': 0}, 'Fairbanks Station': {'zipcode': '02446', 'entries': 0}, 'Lowell': {'zipcode': '01852', 'entries': 0}, 'Fenway Station': {'zipcode': '02215', 'entries': 0}, 'Eliot Station': {'zipcode': '02461', 'entries': 0}, 'Savin Hill Station': {'zipcode': '02125', 'entries': 0}, 'Wyoming Hill': {'zipcode': '02176', 'entries': 0}, 'Prudential Station': {'zipcode': '02199', 'entries': 0}, 'Sutherland Street Station': {'zipcode': '02135', 'entries': 0}, 'Franklin': {'zipcode': '02038', 'entries': 0}, 'Riverway Station': {'zipcode': '02130', 'entries': 0}, 'Mishawum': {'zipcode': '01801', 'entries': 0}, 'Readville': {'zipcode': '02136', 'entries': 0}, 'Brockton': {'zipcode': '02301', 'entries': 0}, 'Quincy Adams Station': {'zipcode': '02169', 'entries': 0}, 'West Natick': {'zipcode': '01760', 'entries': 0}, 'Lechmere Station': {'zipcode': '02141', 'entries': 0}, 'Orient Heights Station': {'zipcode': '02128', 'entries': 0}, 'JFK / UMass Station': {'zipcode': '02125', 'entries': 0}, 'Montello': {'zipcode': '02302', 'entries': 0}, 'North Wilmington': {'zipcode': '01887', 'entries': 0}, 'Canton Junction': {'zipcode': '02021', 'entries': 0}, 'Rockport': {'zipcode': '01966', 'entries': 0}, 'Cohasset Station': {'zipcode': '02025', 'entries': 0}, 'West Newton': {'zipcode': '02465', 'entries': 0}, 'Malden Center Station': {'zipcode': '02148', 'entries': 0}, 'Massachusetts Avenue Station (Orange)': {'zipcode': '02118', 'entries': 0}, 'Nantasket Junction Station': {'zipcode': '02043', 'entries': 0}, 'Manchester': {'zipcode': '01944', 'entries': 0}, 'Central Station': {'zipcode': '02139', 'entries': 0}, 'Waltham': {'zipcode': '02453', 'entries': 0}, 'Plimptonville': {'zipcode': '02081', 'entries': 0}, 'Haverhill': {'zipcode': '01832', 'entries': 0}, 'East Berkeley Street Station': {'zipcode': '02118', 'entries': 0}, 'Massachusetts Avenue Station (Silver)': {'zipcode': '02118', 'entries': 0}, 'St. Paul Street Station (C)': {'zipcode': '02446', 'entries': 0}, 'Ayer': {'zipcode': '01432', 'entries': 0}, 'Quincy Center Station': {'zipcode': '02169', 'entries': 0}, 'Norfolk': {'zipcode': '02056', 'entries': 0}, 'Broadway Station': {'zipcode': '02127', 'entries': 0}, 'Middleborough/Lakeville': {'zipcode': '02347', 'entries': 0}, 'Lawrence': {'zipcode': '01843', 'entries': 0}, 'Waverley': {'zipcode': '02478', 'entries': 0}, 'Anderson/Woburn': {'zipcode': '01801', 'entries': 0}, 'Terminal B Stop 1': {'zipcode': '02128', 'entries': 0}, 'Gloucester': {'zipcode': '01930', 'entries': 0}, 'Melrose Highlands': {'zipcode': '02176', 'entries': 0}, 'Boylston Street Station': {'zipcode': '02116', 'entries': 0}, 'Community College Station': {'zipcode': '02129', 'entries': 0}, 'Bradford': {'zipcode': '01835', 'entries': 0}, 'Brigham Circle Station': {'zipcode': '02115', 'entries': 0}, 'Wilmington': {'zipcode': '01887', 'entries': 0}, 'Warren Street Station': {'zipcode': '02135', 'entries': 0}, 'Terminal B Stop 2': {'zipcode': '02128', 'entries': 0}, 'Uphams Corner': {'zipcode': '02125', 'entries': 0}, 'South Attleboro': {'zipcode': '02703', 'entries': 0}, 'Forge Park/Route 495': {'zipcode': '02038', 'entries': 0}, 'Jackson Square Station': {'zipcode': '02130', 'entries': 0}, 'Grafton': {'zipcode': '01545', 'entries': 0}, 'North Quincy Station': {'zipcode': '02171', 'entries': 0}, 'Summit Avenue (formerly Winchester Street) Station': {'zipcode': '02446', 'entries': 0}, 'Beverly Farms': {'zipcode': '01915', 'entries': 0}, 'Silver Line Way Station': {'zipcode': '02210', 'entries': 0}, 'Canton Center': {'zipcode': '02021', 'entries': 0}, 'Roslindale Village': {'zipcode': '02131', 'entries': 0}, '88 Black Falcon Avenue': {'zipcode': '02210', 'entries': 0}, 'Newton Center Station': {'zipcode': '02459', 'entries': 0}, 'Wellesley Square': {'zipcode': '02482', 'entries': 0}, 'North Scituate Station': {'zipcode': '02066', 'entries': 0}, 'Symphony Station': {'zipcode': '02115', 'entries': 0}, 'Chiswick Road Station': {'zipcode': '02135', 'entries': 0}, 'Attleboro': {'zipcode': '02703', 'entries': 0}, 'Terminal C': {'zipcode': '02128', 'entries': 0}, 'Government Center Station': {'zipcode': '02108', 'entries': 0}, 'Greenwood': {'zipcode': '01880', 'entries': 0}, 'Worcester Square Station': {'zipcode': '02118', 'entries': 0}, 'Braintree Station': {'zipcode': '02184', 'entries': 0}, 'Blandford Street Station': {'zipcode': '02215', 'entries': 0}, 'Beachmont Station': {'zipcode': '02151', 'entries': 0}, 'Herald Street Station': {'zipcode': '02118', 'entries': 0}, 'Newtonville': {'zipcode': '02460', 'entries': 0}, 'South Street Station': {'zipcode': '02135', 'entries': 0}, 'Kingston/Route 3': {'zipcode': '02364', 'entries': 0}, 'Andrew Station': {'zipcode': '02127', 'entries': 0}, 'Hyde Park': {'zipcode': '02136', 'entries': 0}, 'Newton Street Station': {'zipcode': '02118', 'entries': 0}, 'Auburndale': {'zipcode': '02466', 'entries': 0}, 'Milton Station': {'zipcode': '02186', 'entries': 0}, 'Beverly Depot': {'zipcode': '01915', 'entries': 0}, 'River Works': {'zipcode': '01905', 'entries': 0}, 'Mattapan Station': {'zipcode': '02126', 'entries': 0}, 'Salem': {'zipcode': '01970', 'entries': 0}, 'Reading': {'zipcode': '01867', 'entries': 0}, 'North Leominster': {'zipcode': '01453', 'entries': 0}, 'Cedar Grove Station': {'zipcode': '02124', 'entries': 0}, 'Concord': {'zipcode': '01742', 'entries': 0}, 'Hamilton/Wenham': {'zipcode': '01982', 'entries': 0}, 'Belmont Center': {'zipcode': '02478', 'entries': 0}, 'Copley Station': {'zipcode': '02116', 'entries': 0}, 'Kendall/MIT Station': {'zipcode': '02142', 'entries': 0}, 'Brandon Hall Station': {'zipcode': '02446', 'entries': 0}, 'Endicott': {'zipcode': '02026', 'entries': 0}, 'Hastings': {'zipcode': '02493', 'entries': 0}, 'Lenox Street Station': {'zipcode': '02118', 'entries': 0}, 'Stony Brook Station': {'zipcode': '02130', 'entries': 0}, 'Wollaston Station': {'zipcode': '02170', 'entries': 0}, 'Campello': {'zipcode': '02301', 'entries': 0}, 'Route 128': {'zipcode': '02021', 'entries': 0}}

        # For each station in collection dictionary, update with the number of entries
        for doc in ridership:
            docDict2 = dict(doc)
            for station in docDict2['stations']:
                if station['title'] in collection:
                    elem = collection[[station['title']][0]]
                    elem['entries'] = station['entries']
        
        # Convert dictionary into JSON object 
        data = json.dumps(collection, sort_keys=True, indent=2)
        r = json.loads(data)

        # Remove period characters from JSON object because keys with this character are not supported by mongodb
        for key in r.keys():
            new_key = key.replace(".","")
            if new_key != key:
                r[new_key] = r[key]
                del r[key] 

        # Create new dataset called tRidershipLocation
        repo.dropPermanent("tRidershipLocation")
        repo.createPermanent("tRidershipLocation")
        repo['ktan_ngurung.tRidershipLocation'].insert_one(r)
    
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
        repo.authenticate('ktan_ngurung', 'ktan_ngurung')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('ede', 'http://erikdemaine.org/maps/')
        doc.add_namespace('mbt', 'http://www.mbta.com/about_the_mbta/document_library/')

        this_script = doc.agent('alg:ktan_ngurung#landmarkLocations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        tStops_resource = doc.entity('ede:mbta', {'prov:label':'T-Stop Locations', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'yaml'})
        ridership_resource = doc.entity('mbt:?search=blue+book&submit_document_search=Search+Library', {'prov:label':'Boston 2014 Bluebook', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'pdf'})
        this_run = doc.activity('log:a' + str(uuid.uuid4()), startTime, endTime, {prov.model.PROV_TYPE:'ont:Computation'})

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, tStops_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )
        doc.usage(this_run, ridership_resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        ridershipLocation = doc.entity('dat:ktan_ngurung#ridershipLocation', {prov.model.PROV_LABEL:'Number of Entries for Each Train Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(ridershipLocation, this_script)
        doc.wasGeneratedBy(ridershipLocation, this_run, endTime)
        doc.wasDerivedFrom(ridershipLocation, tStops_resource, this_run, this_run, this_run)
        doc.wasDerivedFrom(ridershipLocation, ridership_resource, this_run, this_run, this_run)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc 

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
